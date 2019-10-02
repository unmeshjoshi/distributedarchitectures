package org.dist.queue.common

case class StateChangeFailedException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
  def this() = this(null, null)
}
