package org.dist.queue.client.producer

import org.scalatest.FunSuite

class DefaultPartitionerTest extends FunSuite {

  test("should get random partition for given key") {
    val partitioner = new DefaultPartitioner[String]()
    partitioner.partition("FB", 3) == partitioner.partition("ea", 3)
  }

}
