package org.dist.queue

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{ProducerRequest, RequestOrResponse}

import scala.collection.mutable


class Producer {

  object StringEncoder {
    def toBytes(key:String) = key.getBytes("UTF8")
  }

  val currentCorrelationId: Short = 1

  def send(keyedMessage: KeyedMessage[String, String]): Unit = {
    val serializedMessage = new KeyedMessage[Integer, Message]("topic1",
      new Message(StringEncoder.toBytes(keyedMessage.message), StringEncoder.toBytes(keyedMessage.key), NoCompressionCodec))

    val list = List(serializedMessage)
    val value1: Seq[Message] = list.map(km ⇒ km.message)
    val messageSet = new ByteBufferMessageSet(NoCompressionCodec, new AtomicLong(0), value1)
    val messagesSetPerTopic = new collection.mutable.HashMap[TopicAndPartition, ByteBufferMessageSet] ()
    messagesSetPerTopic.put(TopicAndPartition("topic1", 0), messageSet)
    val producerRequest = ProducerRequest(1,
      "clientid1",
      10, 10, messagesSetPerTopic)

    val map1: mutable.Map[String, ByteBufferMessageSet] = producerRequest.data
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

