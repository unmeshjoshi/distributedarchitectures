package org.dist.queue

case class InvalidMessageSizeException(str: String) extends RuntimeException(str) {

}
