package org.dist.queue

import java.nio.ByteBuffer

import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{ProducerRequest, RequestOrResponse}

import scala.math.max

class Producer {

  object StringEncoder {
    def toBytes(key:String) = key.getBytes
  }

  val currentCorrelationId: Short = 1

  def send(keyedMessage: KeyedMessage[String, String]): Unit = {
    val serializedMessage = new KeyedMessage[Integer, Message]("topic1",
      new Message(StringEncoder.toBytes(keyedMessage.key), StringEncoder.toBytes(keyedMessage.message)))

    val list = List(serializedMessage)
    val messageSet = new ByteBufferMessageSet(NoCompressionCodec, list.map(km ⇒ km.message))
    val messagesSetPerTopic = new collection.mutable.HashMap[TopicAndPartition, ByteBufferMessageSet] ()
    messagesSetPerTopic.put(TopicAndPartition("topic1", 0), messageSet)
    val producerRequest = new ProducerRequest(currentCorrelationId, 1,
      "clientid1",
      10, 10, messagesSetPerTopic)

    val request = RequestOrResponse(1, JsonSerDes.serialize(producerRequest), producerRequest.correlationId)
    val requestBytes = JsonSerDes.serialize(request)

    val reqOrRes = JsonSerDes.deserialize(requestBytes.getBytes, classOf[RequestOrResponse])
    val req = JsonSerDes.deserialize(reqOrRes.messageBodyJson.getBytes, classOf[ProducerRequest])
    assert(producerRequest == req)
  }



}


object Message {
  val KeySizeOffset = 0
  val KeySizeLength = 4
  val KeyOffset = KeySizeOffset + KeySizeLength
  val ValueSizeLength = 4
  /**
   * The current offset and size for all the fixed-length fields
   */
  val CrcOffset = 0
  val CrcLength = 4
  val MagicOffset = CrcOffset + CrcLength
  val MagicLength = 1
  val AttributesOffset = MagicOffset + MagicLength
  val AttributesLength = 1
  //    val KeySizeOffset = AttributesOffset + AttributesLength
  //    val KeySizeLength = 4
  //    val KeyOffset = KeySizeOffset + KeySizeLength
  //    val ValueSizeLength = 4

  /** The amount of overhead bytes in a message */
  val MessageOverhead = KeyOffset + ValueSizeLength

  /**
   * The minimum valid size for the message header
   */
  val MinHeaderSize = CrcLength + MagicLength + AttributesLength + KeySizeLength + ValueSizeLength

  /**
   * The current "magic" value
   */
  val CurrentMagicValue: Byte = 0

  /**
   * Specifies the mask for the compression code. 2 bits to hold the compression codec.
   * 0 is reserved to indicate no compression
   */
  val CompressionCodeMask: Int = 0x03

  /**
   * Compression code for uncompressed messages
   */
  val NoCompression: Int = 0

}

case class Message(val buffer: ByteBuffer) {
  def isValid = true

  def compressionCodec: CompressionCodec = NoCompressionCodec

  //    CompressionCodec.getCompressionCodec(buffer.get(AttributesOffset) & CompressionCodeMask)


  def this(key: Array[Byte], bytes: Array[Byte]) {
    this(ByteBuffer.allocate(Message.KeySizeLength +
      (if (key == null) 0 else key.length) +
      Message.ValueSizeLength +
      bytes.length - 0))

    if (key == null) {
      buffer.putInt(-1)
    } else {
      buffer.putInt(key.length)
      buffer.put(key, 0, key.length)
    }
    val size = bytes.length
    buffer.putInt(size)
    buffer.put(bytes, 0, size)
    buffer.rewind()
  }


  /**
   * The complete serialized size of this message in bytes (including crc, header attributes, etc)
   */
  def size: Int = buffer.limit

  /**
   * Does the message have a key?
   */
  def hasKey: Boolean = keySize >= 0

  /**
   * The length of the key in bytes
   */
  def keySize: Int = buffer.getInt(Message.KeySizeOffset)

  /**
   * The length of the message value in bytes
   */
  def payloadSize: Int = buffer.getInt(payloadSizeOffset)

  /**
   * A ByteBuffer containing the content of the message
   */
  def payload: ByteBuffer = sliceDelimited(payloadSizeOffset)

  /**
   * The position where the payload size is stored
   */
  private def payloadSizeOffset = Message.KeyOffset + max(0, keySize)

  /**
   * A ByteBuffer containing the message key
   */
  def key: ByteBuffer = sliceDelimited(Message.KeySizeOffset)

  /**
   * Read a size-delimited byte buffer starting at the given offset
   */
  private def sliceDelimited(start: Int): ByteBuffer = {
    val size = buffer.getInt(start)
    if (size < 0) {
      null
    } else {
      var b = buffer.duplicate
      b.position(start + 4)
      b = b.slice()
      b.limit(size)
      b.rewind
      b
    }
  }
}