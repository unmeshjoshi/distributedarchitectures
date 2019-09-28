package org.dist.queue

case class NotLeaderForPartitionException(str: String) extends RuntimeException(str) {

}
