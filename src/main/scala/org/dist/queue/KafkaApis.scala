package org.dist.queue

import org.I0Itec.zkclient.ZkClient
import org.dist.kvstore.JsonSerDes
import org.dist.queue.api._
import org.dist.queue.utils.ZkUtils
import org.dist.queue.utils.ZkUtils.Broker

import scala.collection.mutable


class KafkaApis(val replicaManager: ReplicaManager,
                val zkClient: ZkClient,
                brokerId: Int,
                val controller: Controller) extends Logging {
  /* following 3 data structures are updated by the update metadata request
   * and is queried by the topic metadata request. */
  var leaderCache: mutable.Map[TopicAndPartition, PartitionStateInfo] =
    new mutable.HashMap[TopicAndPartition, PartitionStateInfo]()
  private val aliveBrokers: mutable.Map[Int, Broker] = new mutable.HashMap[Int, Broker]()
  private val partitionMetadataLock = new Object

  def handle(req: RequestOrResponse): RequestOrResponse = {
    val request: RequestOrResponse = req
    request.requestId match {
      case RequestKeys.UpdateMetadataKey ⇒ {
        println(s"Handling UpdateMetadataRequest ${request.messageBodyJson}")
        val message = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[UpdateMetadataRequest])
        val response = handleUpdateMetadataRequest(message)
        RequestOrResponse(0, JsonSerDes.serialize(response), request.correlationId)

      }
      case RequestKeys.LeaderAndIsrKey ⇒ {
        println(s"Handling LeaderAndIsrRequest ${request.messageBodyJson}" )
        val leaderAndIsrRequest: LeaderAndIsrRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[LeaderAndIsrRequest])
        val tuple: (collection.Map[(String, Int), Short], Short) = replicaManager.becomeLeaderOrFollower(leaderAndIsrRequest)
        val response = LeaderAndIsrResponse(leaderAndIsrRequest.controllerId, tuple._1, tuple._2)
        RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(response), leaderAndIsrRequest.correlationId)
      }
      case RequestKeys.MetadataKey ⇒ {
        val topicMetadataRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[TopicMetadataRequest])
        val topicMetadataResponse = handleTopicMetadataRequest(topicMetadataRequest)
        RequestOrResponse(RequestKeys.MetadataKey, JsonSerDes.serialize(topicMetadataResponse), topicMetadataRequest.correlationId)
      }
    }
  }

  /**
   * Service the topic metadata request API
   */
  def handleTopicMetadataRequest(metadataRequest: TopicMetadataRequest) = {
    val topicsMetadata = new mutable.ArrayBuffer[TopicMetadata]()
    val config = replicaManager.config
    var uniqueTopics = Set.empty[String]
    uniqueTopics = {
      if (metadataRequest.topics.size > 0)
        metadataRequest.topics.toSet
      else
        ZkUtils.getAllTopics(zkClient).toSet
    }
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
              }// let it go, possibly another broker created this topic
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
    trace("Sending topic metadata %s for correlation id %d to client %s".format(topicsMetadata.mkString(","), metadataRequest.correlationId, metadataRequest.clientId))
    TopicMetadataResponse(topicsMetadata.toSeq, metadataRequest.correlationId)
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
