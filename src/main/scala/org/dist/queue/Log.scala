package org.dist.queue

import java.util

class Log {
  val buffer = new util.ArrayList[Byte]()

  def truncateTo(highWatermark: Long) = {

  }

  def append(bytes:Array[Byte]) = {

  }

  def logEndOffset: Long = 1

}
