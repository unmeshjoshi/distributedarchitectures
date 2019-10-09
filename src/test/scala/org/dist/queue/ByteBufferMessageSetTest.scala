package org.dist.queue

import org.dist.queue.message.{ByteBufferMessageSet, CompressionCodec, DefaultCompressionCodec, Message, NoCompressionCodec}
import org.scalatest.FunSuite

class ByteBufferMessageSetTest extends FunSuite {

  def createMessageSet(messages: Seq[Message], compressed: CompressionCodec = NoCompressionCodec): ByteBufferMessageSet =
    new ByteBufferMessageSet(new ByteBufferMessageSet(compressed, messages.toSeq).buffer)

  val msgSeq: Seq[Message] = Seq(new Message("key1".getBytes(), "hello".getBytes()), new Message("key2".getBytes(), "there".getBytes()))


  private def printKeyValue(value: ByteBufferMessageSet) = {
    for (m <- value.iterator) {
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

  test("testEquals") {
    val messageList = createMessageSet(msgSeq, NoCompressionCodec)
    val moreMessages = createMessageSet(msgSeq, NoCompressionCodec)
    printKeyValue(messageList)
    assert (messageList == moreMessages)
    assert (messageList == moreMessages)
  }


  test("EqualsWithCompression"){
    val messageList = createMessageSet(msgSeq, DefaultCompressionCodec)
    val moreMessages = createMessageSet(msgSeq, DefaultCompressionCodec)
    assert(messageList == moreMessages)
    assert(messageList == moreMessages)
  }
}