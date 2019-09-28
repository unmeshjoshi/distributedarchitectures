package org.dist.queue

import org.scalatest.FunSuite

class ProducerTest extends FunSuite {

  test("should created a serialized message") {
    val producer = new Producer()
    producer.send(KeyedMessage("topic1", "key1", "test message"))
  }

}
