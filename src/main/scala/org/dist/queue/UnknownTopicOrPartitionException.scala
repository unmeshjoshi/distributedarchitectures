package org.dist.queue

case class UnknownTopicOrPartitionException(str: String) extends RuntimeException(str) {

}
