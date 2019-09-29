package org.dist.queue

import java.util.concurrent.atomic.AtomicInteger

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api._
import org.dist.queue.network.SocketClient

import scala.collection.mutable.HashMap
import scala.collection.{Seq, mutable}


class Producer(bootstrapBroker:InetAddressAndPort, config:Config) extends Logging {
  val correlationId = new AtomicInteger(0)
  val clientId = "client1"
  val socketClient = new SocketClient
  val brokerPartitionInfo = new BrokerPartitionInfo(config, bootstrapBroker, new HashMap[String, TopicMetadata]())


  object StringEncoder {
    def toBytes(key:String) = key.getBytes("UTF8")
  }

  def send(keyedMessage: KeyedMessage[String, String]): Unit = {
    val serializedMessage = new KeyedMessage[Integer, Message](keyedMessage.topic,
      new Message(StringEncoder.toBytes(keyedMessage.message), StringEncoder.toBytes(keyedMessage.key), NoCompressionCodec))
    val list = List(serializedMessage)

    val topicMetadata = fetchTopicMetadata(Set(serializedMessage.topic))
    brokerPartitionInfo.updateInfo(Set(keyedMessage.topic), correlationId.getAndIncrement(), topicMetadata)

    val value1: scala.Seq[Message] = list.map(km ⇒ km.message)
    val messageSet = new ByteBufferMessageSet(NoCompressionCodec, value1)
    val messagesSetPerTopic = new collection.mutable.HashMap[TopicAndPartition, ByteBufferMessageSet] ()
    messagesSetPerTopic.put(TopicAndPartition("topic1", 0), messageSet)
    val producerRequest = ProducerRequest(1,
      "clientid1",
      10, 10, messagesSetPerTopic.toMap)
    val map1 = producerRequest.data
    val set = map1("topic1:0")
    printKeyValue(set)




    val request = RequestOrResponse(1, JsonSerDes.serialize(producerRequest), producerRequest.correlationId)
    val requestBytes = JsonSerDes.serialize(request)

    val reqOrRes = JsonSerDes.deserialize(requestBytes.getBytes, classOf[RequestOrResponse])
    val req = JsonSerDes.deserialize(reqOrRes.messageBodyJson.getBytes, classOf[ProducerRequest])

    val data = req.data
    val maybeSet: Option[ByteBufferMessageSet] = data.get("topic1:0")
    val value = maybeSet.get
    printKeyValue(value)
    assert(producerRequest == req)
  }


  def fetchTopicMetadata(topics: Set[String]) = {
    val correlationIdForRequest = correlationId.getAndIncrement()
    val topicMetadataRequest = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, correlationIdForRequest, clientId, topics.toSeq)
    val response = socketClient.sendReceiveTcp(new RequestOrResponse(RequestKeys.MetadataKey, JsonSerDes.serialize(topicMetadataRequest), correlationIdForRequest), bootstrapBroker)
    val topicMetadataResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[TopicMetadataResponse])
    topicMetadataResponse.topicsMetadata
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

