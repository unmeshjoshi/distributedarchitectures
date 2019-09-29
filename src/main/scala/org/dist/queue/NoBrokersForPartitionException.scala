package org.dist.queue

case class NoBrokersForPartitionException(str: String) extends RuntimeException(str) {

}
