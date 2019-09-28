package org.dist.queue

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.ByteBuffer

class MemoryRecords {
  private val bufferStream:ByteBuffer = null
  val appendStream = {
    new DataOutputStream(new ByteArrayOutputStream())
  }

  def append(key:ByteBuffer, value:ByteBuffer): Unit = {

  }
}
