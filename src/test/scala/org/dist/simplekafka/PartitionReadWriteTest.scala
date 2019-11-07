package org.dist.simplekafka

import org.dist.queue.TestUtils
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.util.Networks
import org.scalatest.FunSuite

class PartitionReadWriteTest extends FunSuite {
  test("should write and read messages in partition") {
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), "", List(TestUtils.tempDir().getAbsolutePath))

    val partition = new Partition(config1, TopicAndPartition("topic1", 0))
    val offset = partition.append("key1", "message1")
    val offset1 = partition.append("key2", "message2")
    val messages: Seq[partition.Row] = partition.read()
    assert(messages.size == 2)
    assert(messages(0).key == "key1")
    assert(messages(1).key == "key2")
    assert(messages(0).value == "message1")
    assert(messages(1).value == "message2")
  }

  test("should read messages from specific offset") {
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), "", List(TestUtils.tempDir().getAbsolutePath))

    val partition = new Partition(config1, TopicAndPartition("topic1", 0))
    val offset1 = partition.append("key1", "message1")
    val offset2 = partition.append("key2", "message2")
    val offset3 = partition.append("key3", "message3")
    val messages: Seq[partition.Row] = partition.read(offset2)
    assert(messages.size == 2)
    assert(messages(0).key == "key2")
    assert(messages(0).value == "message2")
    assert(messages(1).key == "key3")
    assert(messages(1).value == "message3")
  }
}
