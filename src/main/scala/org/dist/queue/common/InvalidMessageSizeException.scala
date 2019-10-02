package org.dist.queue.common

case class InvalidMessageSizeException(str: String) extends RuntimeException(str) {

}
