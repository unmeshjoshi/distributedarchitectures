package org.dist.queue.message

case class MessageAndOffset(message: Message, offset: Long) {

  /**
   * Compute the offset of the next message in the log
   */
  def nextOffset: Long = offset + 1
}
