package org.dist.queue

import org.dist.queue.utils.ZkUtils
import org.scalatest.FunSuite

class AdminUtilsTest extends ZookeeperTestHarness {

  test("should create assign replicas to topic partitions") {
    val value: collection.Map[Int, collection.Seq[Int]] = AdminUtils.assignReplicasToBrokers(Seq(0, 1), 1, 1)
    assert(value(0) == List(1))
  }

  test("should register partition assignments in zookeeper") {
    val assignments: collection.Map[Int, collection.Seq[Int]] = AdminUtils.assignReplicasToBrokers(Seq(0, 1), 1, 1)

    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK("topic1",
      assignments, zkClient)

    val storedAssignments: collection.Seq[Int] = ZkUtils.getReplicasForPartition(zkClient, "topic1", 0)

    assert(assignments(0) == storedAssignments)
  }
}
