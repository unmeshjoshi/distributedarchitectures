package org.dist.queue

import org.scalatest.FunSuite

class AdminUtilsTest extends ZookeeperTestHarness {

  test("should create assign replicas to topic partitions") {
    val brokerId1 = 0
    val brokerId2 = 1

    val config1 = Config(brokerId1, TestUtils.hostName(), TestUtils.choosePort(), TestZKUtils.zookeeperConnect)
    val server1 = new Server(config1)
    server1.startup()

    val config2 = Config(brokerId2, TestUtils.hostName(), TestUtils.choosePort(), TestZKUtils.zookeeperConnect)
    val server2 = new Server(config2)
    server2.startup()

    val value: collection.Map[Int, collection.Seq[Int]] = AdminUtils.assignReplicasToBrokers(Seq(0, 1), 1, 1)
    assert(value(0) == List(1))
  }
}
