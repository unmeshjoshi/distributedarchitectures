package org.dist.queue.common

case class UnknownTopicOrPartitionException(str: String) extends RuntimeException(str) {

}
