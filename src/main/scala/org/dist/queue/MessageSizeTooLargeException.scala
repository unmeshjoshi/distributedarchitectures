package org.dist.queue

case class MessageSizeTooLargeException(str: String) extends RuntimeException(str)  {

}
