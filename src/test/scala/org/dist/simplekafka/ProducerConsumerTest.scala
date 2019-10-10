package org.dist.simplekafka

import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks

class ProducerConsumerTest extends ZookeeperTestHarness {
  test("should produce and consumer messages from three broker cluster") {
    val broker1 = newBroker(1)
    val broker2 = newBroker(2)
    val broker3 = newBroker(3)

    broker1.startup()
    broker2.startup()
    broker3.startup()

    TestUtils.waitUntilTrue(()⇒ {
      broker1.controller.liveBrokers.size == 3
    }, "Waiting for all brokers are discovered by controller")

    new CreateTopicCommand(broker1.zookeeperClient).createTopic("topic1", 2, 2)

    TestUtils.waitUntilTrue(() ⇒ {
        liveBrokersIn(broker1) == 3 && liveBrokersIn(broker2) == 3 && liveBrokersIn(broker3) == 3
    }, "waiting till topic metadata is propogated to all the servers", 2000 )

    assert(leaderCache(broker1) ==  leaderCache(broker2) &&  leaderCache(broker2) == leaderCache(broker3))
  }

  private def leaderCache(broker: Server) = {
    broker.socketServer.kafkaApis.leaderCache
  }

  private def liveBrokersIn(broker1: Server) = {
    broker1.socketServer.kafkaApis.aliveBrokers.size
  }

  private def newBroker(brokerId: Int) = {
    val config1 = Config(brokerId, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config1)
    val replicaManager = new ReplicaManager(config1)
    val socketServer1 = new SimpleSocketServer(config1.brokerId, config1.hostName, config1.port, new SimpleKafkaApi(config1, replicaManager))
    val controller = new Controller(zookeeperClient, config1.brokerId, socketServer1)
    new Server(config1, zookeeperClient, controller, socketServer1)
  }
}
