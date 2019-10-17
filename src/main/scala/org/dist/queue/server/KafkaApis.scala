package org.dist.queue.server

import org.I0Itec.zkclient.ZkClient
import org.dist.kvstore.JsonSerDes
import org.dist.queue.admin.CreateTopicCommand
import org.dist.queue.api._
import org.dist.queue.common._
import org.dist.queue.controller.{Controller, PartitionStateInfo}
import org.dist.queue.message.{ByteBufferMessageSet, KeyedMessage, MessageSet}
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.utils.{SystemTime, Utils, ZkUtils}

import scala.collection.{Map, mutable}


class KafkaApis(val replicaManager: ReplicaManager,
                val zkClient: ZkClient,
                brokerId: Int,
                val controller: Controller) extends Logging {
  def close(): Unit = {}

  /* following 3 data structures are updated by the update metadata request
   * and is queried by the topic metadata request. */
  var leaderCache: mutable.Map[TopicAndPartition, PartitionStateInfo] =
    new mutable.HashMap[TopicAndPartition, PartitionStateInfo]()
  private val aliveBrokers: mutable.Map[Int, Broker] = new mutable.HashMap[Int, Broker]()
  private val partitionMetadataLock = new Object

  def handleFetchRequest(fetchRequest: FetchRequest): FetchResponse = {
    val dataRead: Map[TopicAndPartition, FetchResponsePartitionData] = readMessageSets(fetchRequest)
    val bytesReadable = dataRead.values.map(_.messages.sizeInBytes).sum
    if(fetchRequest.maxWait <= 0 ||
      bytesReadable >= fetchRequest.minBytes ||
      fetchRequest.numPartitions <= 0) {
      debug("Returning fetch response %s for fetch request with correlation id %d to client %s"
        .format(dataRead.values.map(_.error).mkString(","), fetchRequest.correlationId, fetchRequest.clientId))
      val map = dataRead.toMap
      val keys = map.keySet

      import java.util

      import scala.jdk.CollectionConverters._

      val topicPartitionMessages = new util.HashMap[TopicAndPartition, PartitionData]
      for(key <- keys) {
        val data = map(key)
        val keyedMessages = data.messages.map(messageAndOffset => {
          val payload = messageAndOffset.message.payload

          val payloadBytes = new Array[Byte](payload.limit)
          payload.get(payloadBytes)

          val messageKey = messageAndOffset.message.key
          val keyBytes = new Array[Byte](messageKey.limit)
          messageKey.get(keyBytes)

          KeyedMessage(key.topic, new String(keyBytes, "UTF-8"), new String(payloadBytes, "UTF-8"))
        }).toList

        val lastOffset = data.messages.toSeq.lastOption match {
          case Some(lastElem) => lastElem.offset
          case None => 0
        }

        topicPartitionMessages.put(key, PartitionData(keyedMessages.toList, lastOffset))
      }
      FetchResponse(fetchRequest.correlationId, topicPartitionMessages.asScala.toMap)

    } else {
      FetchResponse(fetchRequest.correlationId, Map[TopicAndPartition, PartitionData]().toMap)
    }
  }


  /**
   * Read from a single topic/partition at the given offset upto maxSize bytes
   */
  private def readMessageSet(topic: String,
                             partition: Int,
                             offset: Long,
                             maxSize: Int,
                             fromReplicaId: Int): (MessageSet, Long) = {
    // check if the current broker is the leader for the partitions
    val localReplica = if(fromReplicaId == Request.DebuggingConsumerId)
      replicaManager.getReplicaOrException(topic, partition)
    else
      replicaManager.getLeaderReplicaIfLocal(topic, partition)
    trace("Fetching log segment for topic, partition, offset, size = " + (topic, partition, offset, maxSize))
    val maxOffsetOpt = if (fromReplicaId == Request.OrdinaryConsumerId) {
      Some(localReplica.highWatermark)
    } else {
      None
    }
    val messages = localReplica.log match {
      case Some(log) =>
        log.read(offset, maxSize, maxOffsetOpt)
      case None =>
        error("Leader for partition [%s,%d] on broker %d does not have a local log".format(topic, partition, brokerId))
        MessageSet.Empty
    }
    (messages, localReplica.highWatermark)
  }

  /**
   * Read from all the offset details given and return a map of
   * (topic, partition) -> PartitionData
   */
  private def readMessageSets(fetchRequest: FetchRequest):Map[TopicAndPartition, FetchResponsePartitionData] = {
    val isFetchFromFollower = fetchRequest.isFromFollower
    val func = (tuple: (TopicAndPartition, PartitionFetchInfo)) => {
    val topic = tuple._1.topic
    val partition = tuple._1.partition
    val offset = tuple._2.offset
    val fetchSize = tuple._2.fetchSize

    val partitionData: FetchResponsePartitionData =
      try {

        val (messages, highWatermark) = readMessageSet(topic, partition, offset, fetchSize, fetchRequest.replicaId)

        if (!isFetchFromFollower) {
          new FetchResponsePartitionData(ErrorMapping.NoError, highWatermark, messages)
        } else {
          debug("Leader %d for partition [%s,%d] received fetch request from follower %d"
            .format(brokerId, topic, partition, fetchRequest.replicaId))
          new FetchResponsePartitionData(ErrorMapping.NoError, highWatermark, messages)
        }
      } catch {
        // NOTE: Failed fetch requests is not incremented for UnknownTopicOrPartitionException and NotLeaderForPartitionException
        // since failed fetch requests metric is supposed to indicate failure of a broker in handling a fetch request
        // for a partition it is the leader for
        case utpe: UnknownTopicOrPartitionException =>
          warn("Fetch request with correlation id %d from client %s on partition [%s,%d] failed due to %s".format(
            fetchRequest.correlationId, fetchRequest.clientId, topic, partition, utpe.getMessage))
          new FetchResponsePartitionData(ErrorMapping.codeFor(utpe.getClass.asInstanceOf[Class[Throwable]]), -1L, MessageSet.Empty)
        case nle: NotLeaderForPartitionException =>
          warn("Fetch request with correlation id %d from client %s on partition [%s,%d] failed due to %s".format(
            fetchRequest.correlationId, fetchRequest.clientId, topic, partition, nle.getMessage))
          new FetchResponsePartitionData(ErrorMapping.codeFor(nle.getClass.asInstanceOf[Class[Throwable]]), -1L, MessageSet.Empty)
        case t: Throwable =>
          error("Error when processing fetch request for partition [%s,%d] offset %d from %s with correlation id %d"
            .format(topic, partition, offset, if (isFetchFromFollower) "follower" else "consumer", fetchRequest.correlationId), t)
          new FetchResponsePartitionData(ErrorMapping.codeFor(t.getClass.asInstanceOf[Class[Throwable]]), -1L, MessageSet.Empty)
      }
    (TopicAndPartition(topic, partition), partitionData)
  }
    fetchRequest.requestInfoAsMap.map(func)
  }

  def handle(req: RequestOrResponse): RequestOrResponse = {
    val request: RequestOrResponse = req
    request.requestId match {
      case RequestKeys.UpdateMetadataKey => {
        debug(s"Handling UpdateMetadataRequest ${request.messageBodyJson}")
        val message = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[UpdateMetadataRequest])
        val response = handleUpdateMetadataRequest(message)
        RequestOrResponse(0, JsonSerDes.serialize(response), request.correlationId)

      }
      case RequestKeys.LeaderAndIsrKey => {
        debug(s"Handling LeaderAndIsrRequest ${request.messageBodyJson}" )
        val leaderAndIsrRequest: LeaderAndIsrRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[LeaderAndIsrRequest])
        val tuple: (collection.Map[(String, Int), Short], Short) = replicaManager.becomeLeaderOrFollower(leaderAndIsrRequest)
        val response = LeaderAndIsrResponse(leaderAndIsrRequest.controllerId, tuple._1.toMap, tuple._2)
        RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(response), leaderAndIsrRequest.correlationId)
      }
      case RequestKeys.GetMetadataKey => {
        debug(s"Handling MetadataRequest ${request.messageBodyJson}" )
        val topicMetadataRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[TopicMetadataRequest])
        val topicMetadataResponse = handleTopicMetadataRequest(topicMetadataRequest)
        RequestOrResponse(RequestKeys.GetMetadataKey, JsonSerDes.serialize(topicMetadataResponse), topicMetadataRequest.correlationId)
      }
      case RequestKeys.ProduceKey => {
        debug(s"Handling ProduceRequest ${request.messageBodyJson}" )
        val produceRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[ProduceRequest])
        val produceResponse = handleProducerRequest(produceRequest)
        RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(produceResponse), produceRequest.correlationId)
      }
      case RequestKeys.FetchKey => {
        debug(s"Handling FetchRequest ${request.messageBodyJson}")
        val fetchRequest: FetchRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[FetchRequest])
        val fetchResponse = handleFetchRequest(fetchRequest)

        RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(fetchResponse), fetchRequest.correlationId)
      }
      case RequestKeys.FindCoordinatorKey => {
        debug(s"Handling FindCoordinatorRequest ${request.messageBodyJson}")
        val findCoordinatorRequest: FindCoordinatorRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[FindCoordinatorRequest])
        val partitionId = findPartitionHandlingOffsetFor(findCoordinatorRequest)
        val findCoordinatorResponse = partitionMetadataLock synchronized {
          val topicMetadata: Seq[TopicMetadata] = getTopicsMetadata(Set(Topic.GROUP_METADATA_TOPIC_NAME))
          val partitionMetadata = topicMetadata.head.partitionsMetadata.filter(_.partitionId == partitionId)
          val partitionLeader = partitionMetadata.head.leader.get
          FindCoordinatorResponse(partitionLeader.host, partitionLeader.port)
        }
        RequestOrResponse(RequestKeys.FindCoordinatorKey, JsonSerDes.serialize(findCoordinatorResponse), request.correlationId)
      }
    }
  }

  private def findPartitionHandlingOffsetFor(findCoordinatorRequest: FindCoordinatorRequest) = {
    Utils.abs(findCoordinatorRequest.groupId.hashCode) % Topic.groupMetadataTopicPartitionCount
  }

  def appendToLocalLog(producerRequest: ProduceRequest) = {
    val partitionAndData: Map[TopicAndPartition, MessageSet] = producerRequest.dataAsMap
    val func = (tuple: (TopicAndPartition, MessageSet)) => {
      try {
        val topicAndPartition = tuple._1
        val messages = tuple._2

        val partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition)
        val (start, end) =
          partitionOpt match {
            case Some(partition) => partition.appendMessagesToLeader(messages.asInstanceOf[ByteBufferMessageSet])
            case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d"
              .format(topicAndPartition, brokerId))

          }
        trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
          .format(messages.size, topicAndPartition.topic, topicAndPartition.partition, start, end))
        ProduceResult(topicAndPartition, start, end)
      } catch {
        case e: Exception => throw new RuntimeException(e)
      }
    }

    partitionAndData.map(func)
  }


  case class ProduceResult(key: TopicAndPartition, start: Long, end: Long, error: Option[Throwable] = None) {
    def this(key: TopicAndPartition, throwable: Throwable) =
      this(key, -1L, -1L, Some(throwable))

    def errorCode = error match {
      case None => ErrorMapping.NoError
      case Some(error) => ErrorMapping.UnknownCode
    }
  }

  private [queue] case class RequestKey(topic: String, partition: Int) {

    def this(topicAndPartition: TopicAndPartition) = this(topicAndPartition.topic, topicAndPartition.partition)

    def topicAndPartition = TopicAndPartition(topic, partition)

    def keyLabel = "%s-%d".format(topic, partition)
  }

  def handleProducerRequest(produceRequest:ProduceRequest):ProducerResponse = {
    val sTime = SystemTime.milliseconds
    val localProduceResults = appendToLocalLog(produceRequest)
    debug("Produce to local log in %d ms".format(SystemTime.milliseconds - sTime))

    val numPartitionsInError = localProduceResults.count(_.error.isDefined)
//    produceRequest.dataAsMap.foreach((partitionAndData =>
//      maybeUnblockDelayedFetchRequests(partitionAndData._1.topic, partitionAndData._1.partition, partitionAndData._2.sizeInBytes))

    val allPartitionHaveReplicationFactorOne =
      !produceRequest.dataAsMap.keySet.exists(
        m => replicaManager.getReplicationFactorForPartition(m.topic, m.partition) != 1)
    if(produceRequest.requiredAcks == 0) {
      // no operation needed if producer request.required.acks = 0; however, if there is any exception in handling the request, since
      // no response is expected by the producer the handler will send a close connection response to the socket server
      // to close the socket so that the producer client will know that some exception has happened and will refresh its metadata
      if (numPartitionsInError != 0) {
        info(("Send the close connection response due to error handling produce request " +
          "[clientId = %s, correlationId = %s, topicAndPartition = %s] with Ack=0")
          .format(produceRequest.clientId, produceRequest.correlationId, produceRequest.topicPartitionMessageSizeMap.keySet.mkString(",")))
//        requestChannel.closeConnection(request.processor, request)
      } else {
//        requestChannel.noOperation(request.processor, request)
      }
      ProducerResponse(produceRequest.correlationId, Map[TopicAndPartition, ProducerResponseStatus]().toMap)

    } else if (produceRequest.requiredAcks == 1 ||
      produceRequest.numPartitions <= 0 ||
      allPartitionHaveReplicationFactorOne ||
      numPartitionsInError == produceRequest.numPartitions) {
      val statuses = localProduceResults.map(r => r.key -> ProducerResponseStatus(r.errorCode, r.start)).toMap
      ProducerResponse(produceRequest.correlationId, statuses)

    } else {
      // create a list of (topic, partition) pairs to use as keys for this delayed request
      val producerRequestKeys = produceRequest.dataAsMap.keys.map(
        topicAndPartition => new RequestKey(topicAndPartition)).toSeq
      val statuses = localProduceResults.map(r => r.key -> ProducerResponseStatus(r.errorCode, r.end + 1)).toMap
//      val delayedProduce = new DelayedProduce(producerRequestKeys,
//        produceRequest,
//        statuses,
//        produceRequest,
//        produceRequest.ackTimeoutMs.toLong)
//      producerRequestPurgatory.watch(delayedProduce)

      /*
       * Replica fetch requests may have arrived (and potentially satisfied)
       * delayedProduce requests while they were being added to the purgatory.
       * Here, we explicitly check if any of them can be satisfied.
       */
//      var satisfiedProduceRequests = new mutable.ArrayBuffer[DelayedProduce]
//      producerRequestKeys.foreach(key =>
//        satisfiedProduceRequests ++=
//          producerRequestPurgatory.update(key, key))
//      debug(satisfiedProduceRequests.size +
//        " producer requests unblocked during produce to local log.")
//      satisfiedProduceRequests.foreach(_.respond())
      // we do not need the data anymore
      produceRequest.emptyData()
      ProducerResponse(produceRequest.correlationId, Map[TopicAndPartition, ProducerResponseStatus]().toMap)
    }
  }

  /**
   * Service the topic metadata request API
   */
  def handleTopicMetadataRequest(metadataRequest: TopicMetadataRequest) = {
    var uniqueTopics = Set.empty[String]
    uniqueTopics = {
      if (metadataRequest.topics.size > 0)
        metadataRequest.topics.toSet
      else
        ZkUtils.getAllTopics(zkClient).toSet
    }
    val topicsMetadata = getTopicsMetadata(uniqueTopics)
    trace("Sending topic metadata %s for correlation id %d to client %s".format(topicsMetadata.mkString(","), metadataRequest.correlationId, metadataRequest.clientId))
    TopicMetadataResponse(topicsMetadata, metadataRequest.correlationId)
  }

  private def getTopicsMetadata(uniqueTopics: Set[String]) = {
    val config = replicaManager.config
    val topicsMetadata = new mutable.ArrayBuffer[TopicMetadata]()
    val topicMetadataList =
      partitionMetadataLock synchronized {
        uniqueTopics.map { topic =>
          if (leaderCache.keySet.map(_.topic).contains(topic)) {
            val partitionStateInfo = leaderCache.filter(p => p._1.topic.equals(topic))
            val sortedPartitions = partitionStateInfo.toList.sortWith((m1, m2) => m1._1.partition < m2._1.partition)
            val partitionMetadata = sortedPartitions.map { case (topicAndPartition, partitionState) =>
              val replicas = leaderCache(topicAndPartition).allReplicas
              var replicaInfo: Seq[Broker] = replicas.map(aliveBrokers.getOrElse(_, null)).filter(_ != null).toSeq
              var leaderInfo: Option[Broker] = None
              var isrInfo: Seq[Broker] = Nil
              val leaderIsrAndEpoch = partitionState.leaderIsrAndControllerEpoch
              val leader = leaderIsrAndEpoch.leaderAndIsr.leader
              val isr = leaderIsrAndEpoch.leaderAndIsr.isr
              debug("%s".format(topicAndPartition) + ";replicas = " + replicas + ", in sync replicas = " + isr + ", leader = " + leader)
              try {
                if (aliveBrokers.keySet.contains(leader))
                  leaderInfo = Some(aliveBrokers(leader))
                else throw new LeaderNotAvailableException("Leader not available for partition %s".format(topicAndPartition))
                isrInfo = isr.map(aliveBrokers.getOrElse(_, null)).filter(_ != null)
                if (replicaInfo.size < replicas.size)
                  throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
                    replicas.filterNot(replicaInfo.map(_.id).contains(_)).mkString(","))
                if (isrInfo.size < isr.size)
                  throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
                    isr.filterNot(isrInfo.map(_.id).contains(_)).mkString(","))
                new PartitionMetadata(topicAndPartition.partition, leaderInfo, replicaInfo, isrInfo, ErrorMapping.NoError)
              } catch {
                case e: Throwable =>
                  error("Error while fetching metadata for partition %s".format(topicAndPartition), e)
                  new PartitionMetadata(topicAndPartition.partition, leaderInfo, replicaInfo, isrInfo,
                    ErrorMapping.UnknownTopicOrPartitionCode)
              }
            }
            TopicMetadata(topic, partitionMetadata)
          } else {
            // topic doesn't exist, send appropriate error code
            TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.UnknownTopicOrPartitionCode)
          }
        }
      }

    // handle auto create topics
    topicMetadataList.foreach { topicMetadata =>
      topicMetadata.errorCode match {
        case ErrorMapping.NoError => topicsMetadata += topicMetadata
        case ErrorMapping.UnknownTopicOrPartitionCode =>
          if (config.autoCreateTopicsEnable) {
            try {
              CreateTopicCommand.createTopic(zkClient, topicMetadata.topic, config.numPartitions, config.defaultReplicationFactor)
              info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                .format(topicMetadata.topic, config.numPartitions, config.defaultReplicationFactor))
            } catch {
              case e: Exception => {
                e.printStackTrace()
              } // let it go, possibly another broker created this topic
            }
            topicsMetadata += new TopicMetadata(topicMetadata.topic, topicMetadata.partitionsMetadata, ErrorMapping.LeaderNotAvailableCode)
          } else {
            topicsMetadata += topicMetadata
          }
        case _ =>
          debug("Error while fetching topic metadata for topic %s due to %s ".format(topicMetadata.topic,
            ErrorMapping.UnknownCode))
          topicsMetadata += topicMetadata
      }
    }
    topicsMetadata.toSeq
  }

  def handleUpdateMetadataRequest(updateMetadataRequest: UpdateMetadataRequest) {
    if (updateMetadataRequest.controllerEpoch < replicaManager.controllerEpoch) {
      val stateControllerEpochErrorMessage = ("Broker %d received update metadata request with correlation id %d from an " +
        "old controller %d with epoch %d. Latest known controller epoch is %d").format(brokerId,
        updateMetadataRequest.correlationId, updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch,
        replicaManager.controllerEpoch)
      warn(stateControllerEpochErrorMessage)
      throw new RuntimeException(stateControllerEpochErrorMessage)
    }
    partitionMetadataLock synchronized {
      replicaManager.controllerEpoch = updateMetadataRequest.controllerEpoch
      // cache the list of alive brokers in the cluster
      updateMetadataRequest.aliveBrokers.foreach(b => aliveBrokers.put(b.id, b))
      updateMetadataRequest.partitionStateInfoMap.foreach { partitionState =>
        leaderCache.put(partitionState._1, partitionState._2)

        trace(("Broker %d cached leader info %s for partition %s in response to UpdateMetadata request " +
          "sent by controller %d epoch %d with correlation id %d").format(brokerId, partitionState._2, partitionState._1,
          updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch, updateMetadataRequest.correlationId))
      }
    }
    new UpdateMetadataResponse(updateMetadataRequest.correlationId)
  }
}
