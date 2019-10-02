package org.dist.queue

import org.dist.queue.server.{Config, Server}

class TopicCreationTest extends ZookeeperTestHarness {
  test("should register topic and assign partitions") {
    val brokerId1 = 0
    val brokerId2 = 1

    val config1 = Config(brokerId1, TestUtils.hostName(), TestUtils.choosePort(), TestZKUtils.zookeeperConnect, List(TestUtils.tempDir().getAbsolutePath))
    val server1 = new Server(config1)
    server1.startup()

    val config2 = Config(brokerId2, TestUtils.hostName(), TestUtils.choosePort(), TestZKUtils.zookeeperConnect, List(TestUtils.tempDir().getAbsolutePath))
    val server2 = new Server(config2)
    server2.startup()



  }
}
