package org.dist.queue

import org.dist.queue.utils.ZkUtils

class ServerTest extends ZookeeperTestHarness {

  test("should register broker to zookeeper on startup") {
    val brokerId1 = 0
    val brokerId2 = 1

    val config1 = Config(brokerId1, TestUtils.hostName(), TestUtils.choosePort(), TestZKUtils.zookeeperConnect)
    val server1 = new Server(config1)
    server1.startup()

    val config2 = Config(brokerId2, TestUtils.hostName(), TestUtils.choosePort(), TestZKUtils.zookeeperConnect)
    val server2 = new Server(config2)
    server2.startup()

    val zkClient = KafkaZookeeperClient.getZookeeperClient(config1)

    val brokers: collection.Seq[ZkUtils.Broker] = ZkUtils.getAllBrokersInCluster(zkClient)

    assert(2 == brokers.size)

    val sortedBrokers = brokers.sortWith((b1, b2) ⇒ b1.id < b2.id)

    assert(sortedBrokers(0).id == config1.brokerId)
    assert(sortedBrokers(0).host == config1.hostName)
    assert(sortedBrokers(0).port == config1.port)

    assert(sortedBrokers(1).id == config2.brokerId)
    assert(sortedBrokers(1).host == config2.hostName)
    assert(sortedBrokers(1).port == config2.port)

    Thread.sleep(10000)
  }
}
