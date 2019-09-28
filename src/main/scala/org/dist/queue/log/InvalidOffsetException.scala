package org.dist.queue.log

case class InvalidOffsetException(str: String) extends RuntimeException(str) {

}
