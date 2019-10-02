package org.dist.queue.common

import java.io.IOException

case class KafkaStorageException(str: String, e: IOException) extends RuntimeException(str, e) {

  def this(str:String) = {
    this(str, null)
  }
}
