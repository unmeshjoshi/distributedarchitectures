package org.dist.queue.common

case class NoBrokersForPartitionException(str: String) extends RuntimeException(str) {

}
