package org.dist.queue.common

case class NotLeaderForPartitionException(str: String) extends RuntimeException(str) {

}
