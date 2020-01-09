package org.dist.simplekafka

import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks

import scala.jdk.CollectionConverters._


class ControllerZookeeperFailureTests extends ZookeeperTestHarness {
  test("should elect new broker as leader once controller shuts down") {
    val broker1 = newBroker(1)
    val broker2 = newBroker(2)
    val broker3 = newBroker(3)

    val allBrokers = Set(broker1, broker2, broker3)

    broker1.startup()
    broker2.startup()
    broker3.startup()

    val controller = broker1.controller

    TestUtils.waitUntilTrue(() => {
      controller.liveBrokers.size == 3
    }, "Waiting for all brokers to get added")

    assert(controller.liveBrokers.size == 3)

    val createCommandTest = new CreateTopicCommand(broker1.zookeeperClient)
    createCommandTest.createTopic("topic1", 2, 2)

    TestUtils.waitUntilTrue(() ⇒ {
      println(testSocketServer(broker1).messages.size)
      testSocketServer(broker1).messages.size == 6 && testSocketServer(broker1).toAddresses.asScala.toSet.size == 3
    }, "waiting for leader and replica requests handled in all brokers")

    println("old partitionReplicaLeaderInfo:::"+ broker2.zookeeperClient.getPartitionReplicaLeaderInfo("topic1"))

    broker1.shutdown()

    TestUtils.waitUntilTrue(() ⇒ {
      broker2.controller.currentLeader != 1 && broker3.controller.currentLeader != 1
    }, "Waiting till new leader is elected")

    print("leader partition after shutdown:" + broker2.controller.zookeeperClient.getPartitionReplicaLeaderInfo("topic1"))

    assert(broker2.controller.currentLeader == broker3.controller.currentLeader && broker2.controller.currentLeader != 1)

    val reelectedController = allBrokers.filter(b ⇒ b.config.brokerId == broker2.controller.currentLeader).head
    assert(reelectedController.controller.liveBrokers.size == 2)

    val partitionReplicaLeaderInfo:List[LeaderAndReplicas] = broker2.zookeeperClient.getPartitionReplicaLeaderInfo("topic1");
    partitionReplicaLeaderInfo.foreach(leaderAndReplicas => {
      assert(leaderAndReplicas.partitionStateInfo.leader.id != 1)
    })
  }

  def testSocketServer(server: Server) = {
    server.socketServer.asInstanceOf[TestSocketServer]
  }

  private def newBroker(brokerId: Int) = {
    val config = Config(brokerId, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config)
    val replicaManager = new ReplicaManager(config)
    val socketServer1 = new TestSocketServer(config)
    val controller = new Controller(zookeeperClient, config.brokerId, socketServer1)
    new Server(config, zookeeperClient, controller, socketServer1)
  }
}
