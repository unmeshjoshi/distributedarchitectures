package org.dist.simplekafka

import akka.actor.ActorSystem
import org.dist.queue.TestUtils
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.util.Networks
import org.scalatest.FunSuite

import scala.util.Random

class PartitionConcurrentReadWriteTest extends FunSuite {
  implicit val partitionActorSystem = ActorSystem("partitionActorSystem")

  test("Concurrent write to partition should be serialized") {
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), "", List(TestUtils.tempDir().getAbsolutePath))

    val partition = new Partition(config1, TopicAndPartition("topic1", 0))
    (1 to 100).foreach(i ⇒ {
      new Thread(() => {
        partition.append2(s"key${i}", s"message${i}")
      }).run()
    })

    val messages: Seq[Any] = partition.read(0)
    assert(messages.size == 100)
  }

}
