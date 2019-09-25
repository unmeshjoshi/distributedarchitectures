package org.dist.queue.network

import java.nio._
import java.nio.channels._

import org.dist.queue.{Logging, Utils}

private[queue] class BoundedByteBufferReceive(val maxSize: Int) extends Receive with Logging {

  private val sizeBuffer = ByteBuffer.allocate(4)
  private var contentBuffer: ByteBuffer = null

  def this() = this(Int.MaxValue)

  var complete: Boolean = false

  /**
   * Get the content buffer for this transmission
   */
  def buffer: ByteBuffer = {
    expectComplete()
    contentBuffer
  }

  /**
   * Read the bytes in this response from the given channel
   */
  def readFrom(channel: ReadableByteChannel): Int = {
    expectIncomplete()
    var read = 0
    // have we read the request size yet?
    if(sizeBuffer.remaining > 0)
      read += Utils.read(channel, sizeBuffer)
    // have we allocated the request buffer yet?
    if(contentBuffer == null && !sizeBuffer.hasRemaining) {
      sizeBuffer.rewind()
      val size = sizeBuffer.getInt()
      if(size <= 0)
        throw new InvalidRequestException("%d is not a valid request size.".format(size))
      if(size > maxSize)
        throw new InvalidRequestException("Request of length %d is not valid, it is larger than the maximum size of %d bytes.".format(size, maxSize))
      contentBuffer = byteBufferAllocate(size)
    }
    // if we have a buffer read some stuff into it
    if(contentBuffer != null) {
      read = Utils.read(channel, contentBuffer)
      // did we get everything?
      if(!contentBuffer.hasRemaining) {
        contentBuffer.rewind()
        complete = true
      }
    }
    read
  }

  private def byteBufferAllocate(size: Int): ByteBuffer = {
    var buffer: ByteBuffer = null
    try {
      buffer = ByteBuffer.allocate(size)
    } catch {
      case e: OutOfMemoryError =>
        error("OOME with size " + size, e)
        throw e
      case e2: Throwable =>
        throw e2
    }
    buffer
  }
}
