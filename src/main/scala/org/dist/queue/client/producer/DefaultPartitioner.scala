package org.dist.queue.client.producer

import org.dist.queue.utils.Utils

class DefaultPartitioner[T] extends Partitioner[T] {
  private val random = new java.util.Random

  def partition(key: T, numPartitions: Int): Int = {
    Utils.abs(key.hashCode) % numPartitions
  }
}

