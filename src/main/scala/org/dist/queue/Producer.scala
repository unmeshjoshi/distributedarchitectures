package org.dist.queue

import java.util.concurrent.atomic.AtomicInteger

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api._
import org.dist.queue.network.SocketClient

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.collection.{Map, Seq, mutable}
import scala.util.Random


class Producer(bootstrapBroker:InetAddressAndPort, config:Config, private val partitioner: Partitioner[String]) extends Logging {
  val correlationId = new AtomicInteger(0)
  val clientId = "Producer"
  val socketClient = new SocketClient

  val brokerPartitionInfo = new BrokerPartitionInfo(config,
    bootstrapBroker,
    new HashMap[String, TopicMetadata]())

  private val sendPartitionPerTopicCache = HashMap.empty[String, Int]


  object StringEncoder {
    def toBytes(key:String) = key.getBytes("UTF8")
  }

  /**
   * Retrieves the partition id and throws an UnknownTopicOrPartitionException if
   * the value of partition is not between 0 and numPartitions-1
   * @param key the partition key
   * @param topicPartitionList the list of available partitions
   * @return the partition id
   */
  private def getPartition(topic: String, key: String, topicPartitionList: Seq[PartitionAndLeader]): Int = {
    val numPartitions = topicPartitionList.size
    if(numPartitions <= 0)
      throw new UnknownTopicOrPartitionException("Topic " + topic + " doesn't exist")
    val partition =
      if(key == null) {
        // If the key is null, we don't really need a partitioner
        // So we look up in the send partition cache for the topic to decide the target partition
        val id = sendPartitionPerTopicCache.get(topic)
        id match {
          case Some(partitionId) =>
            // directly return the partitionId without checking availability of the leader,
            // since we want to postpone the failure until the send operation anyways
            partitionId
          case None =>
            val availablePartitions = topicPartitionList.filter(_.leaderBrokerIdOpt.isDefined)
            if (availablePartitions.isEmpty)
              throw new LeaderNotAvailableException("No leader for any partition in topic " + topic)
            val index = Utils.abs(Random.nextInt) % availablePartitions.size
            val partitionId = availablePartitions(index).partitionId
            sendPartitionPerTopicCache.put(topic, partitionId)
            partitionId
        }
      } else
        partitioner.partition(key, numPartitions)
    if(partition < 0 || partition >= numPartitions)
      throw new UnknownTopicOrPartitionException("Invalid partition id: " + partition + " for topic " + topic +
        "; Valid values are in the inclusive range of [0, " + (numPartitions-1) + "]")
    trace("Assigning message of topic %s and key %s to a selected partition %d".format(topic, if (key == null) "[none]" else key.toString, partition))
    partition
  }

  private def groupMessagesToSet(messagesPerTopicAndPartition: collection.mutable.Map[TopicAndPartition, Seq[KeyedMessage[String,Message]]]) = {
    val func = (tuple:(TopicAndPartition, Seq[KeyedMessage[String,Message]])) ⇒ {
      val topicAndPartition = tuple._1
      val messages = tuple._2
      val rawMessages = messages.map(_.message)
      ( topicAndPartition,
        config.compressionCodec match {
          case NoCompressionCodec =>
            debug("Sending %d messages with no compression to %s".format(messages.size, topicAndPartition))
            new ByteBufferMessageSet(NoCompressionCodec, rawMessages.toSeq)
          case _ =>
            throw new RuntimeException("No compression supported")
        }
      )
    }
    val messagesPerTopicPartition: mutable.Map[TopicAndPartition, ByteBufferMessageSet] = messagesPerTopicAndPartition.map(func)
    messagesPerTopicPartition
  }


  def sendToBroker(brokerid: Int, messagesPerTopic: mutable.Map[TopicAndPartition, ByteBufferMessageSet]) = {

    val broker = brokerPartitionInfo.getBroker(brokerid).get

    val correlationIdForRequest = correlationId.getAndIncrement()
    val requiredAcks:Short = 1
    val producerRequest = ProducerRequest(correlationIdForRequest,
      "client1", requiredAcks, config.controllerSocketTimeoutMs, messagesPerTopic.toMap)

    val requestMessage = new RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(producerRequest), correlationIdForRequest)
    val response = socketClient.sendReceiveTcp(requestMessage, InetAddressAndPort.create(broker.host, broker.port))

    var failedTopicPartitions = List.empty[TopicAndPartition]
    failedTopicPartitions
  }

  def dispatchSerializedData(messageList: List[KeyedMessage[String, Message]]) = {
    val partitionedDataOpt = partitionAndCollate(messageList)
    partitionedDataOpt match {
      case Some(partitionedData) =>
        val failedProduceRequests: mutable.Seq[KeyedMessage[String, Message]] = new ArrayBuffer[KeyedMessage[String,Message]]
        try {
          for ((brokerid, messagesPerBrokerMap) <- partitionedData) {
            if (logger.isTraceEnabled)
              messagesPerBrokerMap.foreach(partitionAndEvent =>
                trace("Handling event for Topic: %s, Broker: %d, Partitions: %s".format(partitionAndEvent._1, brokerid, partitionAndEvent._2)))
            val messageSetPerBroker = groupMessagesToSet(messagesPerBrokerMap)

            val failedTopicPartitions = sendToBroker(brokerid, messageSetPerBroker)
            failedTopicPartitions.foreach(topicPartition => {
              messagesPerBrokerMap.get(topicPartition) match {
                case Some(data) => failedProduceRequests.appendedAll(data)
                case None => // nothing
              }
            })
          }
        } catch {
          case t: Throwable => error("Failed to send messages", t)
        }
        failedProduceRequests
      case None => // all produce requests failed
        messageList
    }
  }


  def send(keyedMessage: KeyedMessage[String, String]): Unit = {
    val correlationIdForReq = correlationId.getAndIncrement()

    val serializedMessage: KeyedMessage[String, Message] = serializeMessage(keyedMessage)

    val messageList = List(serializedMessage)
    val topicMetadata = new ClientUtils().fetchTopicMetadata(Set(serializedMessage.topic), correlationIdForReq, clientId, bootstrapBroker)
    brokerPartitionInfo.updateInfo(Set(keyedMessage.topic), correlationIdForReq, topicMetadata)
    dispatchSerializedData(messageList)

//    printKeyValue(value)
//    assert(producerRequest == req)
  }

  private def serializeMessage(keyedMessage: KeyedMessage[String, String]) = {
    new KeyedMessage[String, Message](keyedMessage.topic,
      new Message(StringEncoder.toBytes(keyedMessage.message), StringEncoder.toBytes(keyedMessage.key), NoCompressionCodec))
  }

  def partitionAndCollate(messages: Seq[KeyedMessage[String,Message]]):Option[Map[Int, collection.mutable.Map[TopicAndPartition, Seq[KeyedMessage[String,Message]]]]] = {
    val ret = new HashMap[Int, collection.mutable.Map[TopicAndPartition, Seq[KeyedMessage[String, Message]]]]
    try {
      for (keyedMessage <- messages) {
        val partitionAndLeader  = getPartitionListForTopic(keyedMessage)
        val partitionIndex = getPartition(keyedMessage.topic, keyedMessage.key, partitionAndLeader)
        val brokerPartition = partitionAndLeader(partitionIndex)
        val leaderBrokerId = brokerPartition.leaderBrokerIdOpt.getOrElse(-1)
        var dataPerBroker: HashMap[TopicAndPartition, Seq[KeyedMessage[String, Message]]] = null
        ret.get(leaderBrokerId) match {
          case Some(element) =>
            dataPerBroker = element.asInstanceOf[HashMap[TopicAndPartition, Seq[KeyedMessage[String, Message]]]]
          case None =>
            dataPerBroker = new HashMap[TopicAndPartition, Seq[KeyedMessage[String, Message]]]
            ret.put(leaderBrokerId, dataPerBroker)
        }
        val topicAndPartition = TopicAndPartition(keyedMessage.topic, brokerPartition.partitionId)
        var dataPerTopicPartition: ArrayBuffer[KeyedMessage[String,Message]] = null
        dataPerBroker.get(topicAndPartition) match {
          case Some(element) =>
            dataPerTopicPartition = element.asInstanceOf[ArrayBuffer[KeyedMessage[String,Message]]]
          case None =>
            dataPerTopicPartition = new ArrayBuffer[KeyedMessage[String,Message]]
            dataPerBroker.put(topicAndPartition, dataPerTopicPartition)
        }
        dataPerTopicPartition.append(keyedMessage)
      }
      Some(ret)
    } catch {    // Swallow recoverable exceptions and return None so that they can be retried.
      case ute: UnknownTopicOrPartitionException => warn("Failed to collate messages by topic,partition due to: " + ute.getMessage); None
      case lnae: LeaderNotAvailableException => warn("Failed to collate messages by topic,partition due to: " + lnae.getMessage); None
      case oe: Throwable => error("Failed to collate messages by topic, partition due to: " + oe.getMessage); None
    }


  }





  private def getPartitionListForTopic(m: KeyedMessage[String,Message]): Seq[PartitionAndLeader] = {
    val topicPartitionsList = brokerPartitionInfo.getBrokerPartitionInfo(m.topic, correlationId.getAndIncrement)
    debug("Broker partitions registered for topic: %s are %s"
      .format(m.topic, topicPartitionsList.map(p => p.partitionId).mkString(",")))
    val totalNumPartitions = topicPartitionsList.length
    if(totalNumPartitions == 0)
      throw new NoBrokersForPartitionException("Partition key = " + m.key)
    topicPartitionsList
  }

  private def printKeyValue(value: ByteBufferMessageSet) = {
    for (m ← value.iterator) {
      //      val message = m.message
      //      val value: ByteBuffer = message.payload
      //      val key: ByteBuffer = message.key
      //      println(s"${new String(key.array())}->${new String(value.array())}")

      val payload = m.message.payload
      val bytes = new Array[Byte](payload.limit)
      payload.get(bytes)

      val key = m.message.key
      val keyBytes = new Array[Byte](key.limit)
      key.get(keyBytes)

      println(new String(keyBytes, "UTF8"))
      println(new String(bytes, "UTF8"))
    }

  }
}

