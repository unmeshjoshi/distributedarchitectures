package org.dist.queue.controller

import org.dist.queue.server.{Config, Server}
import org.dist.queue.{TestUtils, TestZKUtils, ZookeeperTestHarness}

class ReplicaStateMachineTest extends ZookeeperTestHarness {

  test("should initialize replica state for online replicas") {
    val brokerId1 = 0
    val brokerId2 = 1
    val brokerId3 = 2

    val config1 = Config(brokerId1, TestUtils.hostName(), TestUtils.choosePort(), TestZKUtils.zookeeperConnect, List(TestUtils.tempDir().getAbsolutePath))
    val server1 = new Server(config1)
    server1.startup()

    val config2 = Config(brokerId2, TestUtils.hostName(), TestUtils.choosePort(), TestZKUtils.zookeeperConnect, List(TestUtils.tempDir().getAbsolutePath))
    val server2 = new Server(config2)
    server2.startup()

    val config3 = Config(brokerId3, TestUtils.hostName(), TestUtils.choosePort(), TestZKUtils.zookeeperConnect, List(TestUtils.tempDir().getAbsolutePath))
    val server3 = new Server(config3)
    server3.startup()

    val brokers = Map(brokerId1 → server1, brokerId2 → server2, brokerId3 → server3)


  }
}
