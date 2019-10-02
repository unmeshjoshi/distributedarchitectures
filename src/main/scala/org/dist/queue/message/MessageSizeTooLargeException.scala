package org.dist.queue.message

case class MessageSizeTooLargeException(str: String) extends RuntimeException(str)  {

}
