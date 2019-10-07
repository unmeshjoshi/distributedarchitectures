package org.dist.queue.client.producer

import org.scalatest.FunSuite

class DefaultPartitionerTest extends FunSuite {

  test("should get random partition if for the given number of partitions") {
    val partitioner = new DefaultPartitioner[String]()
    val partitionId = partitioner.partition("FB", 3)
    assert(partitionId < 3)
  }

  test("should get different partition id for keys with different hashcodes") {
    val partitioner = new DefaultPartitioner[String]()
    assert(partitioner.partition("FB", 3) != partitioner.partition("ABC", 3))
  }

  test("Gets same partition if hashcode is same") {
    val partitioner = new DefaultPartitioner[String]()
    partitioner.partition("FB", 3) == partitioner.partition("ea", 3)
  }

}
