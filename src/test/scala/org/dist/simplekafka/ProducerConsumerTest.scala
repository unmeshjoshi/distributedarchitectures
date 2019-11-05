package org.dist.simplekafka

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.common.Logging
import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks

class ProducerConsumerTest extends ZookeeperTestHarness with Logging {

  test("should produce and consumer messages from five broker cluster") {
    val broker1 = newBroker(1)
    val broker2 = newBroker(2)
    val broker3 = newBroker(3)
    val broker4 = newBroker(4)
    val broker5 = newBroker(5)

    broker1.startup() //broker1 will become controller as its the first one to start
    broker2.startup()
    broker3.startup()
    broker4.startup()
    broker5.startup()

    TestUtils.waitUntilTrue(()⇒ {
      broker1.controller.liveBrokers.size == 5
    }, "Waiting for all brokers to be discovered by the controller")

    new CreateTopicCommand(broker1.zookeeperClient).createTopic("topic1", 2, 5)

    TestUtils.waitUntilTrue(() ⇒ {
        liveBrokersIn(broker1) == 5 && liveBrokersIn(broker2) == 5 && liveBrokersIn(broker3) == 5
    }, "waiting till topic metadata is propogated to all the servers", 2000 )

    assert(leaderCache(broker1) ==  leaderCache(broker2) &&  leaderCache(broker2) == leaderCache(broker3))




    val bootstrapBroker = InetAddressAndPort.create(broker2.config.hostName, broker2.config.port)
    val simpleProducer = new SimpleProducer(bootstrapBroker)
    val offset1 = simpleProducer.produce("topic1", "key1", "message1")
    assert(offset1 == 1) //first offset

    val offset2 = simpleProducer.produce("topic1", "key2", "message2")
    assert(offset2 == 1) //first offset on different partition

    val offset3 = simpleProducer.produce("topic1", "key3", "message3")

    assert(offset3 == 2) //offset on first partition

    val simpleConsumer = new SimpleConsumer(bootstrapBroker)
    val messages = simpleConsumer.consume("topic1")

    assert(messages.size() == 3)
    assert(messages.get("key1") == "message1")
    assert(messages.get("key2") == "message2")
    assert(messages.get("key3") == "message3")
  }

  private def leaderCache(broker: Server) = {
    broker.socketServer.kafkaApis.leaderCache
  }

  private def liveBrokersIn(broker1: Server) = {
    broker1.socketServer.kafkaApis.aliveBrokers.size
  }

  private def newBroker(brokerId: Int) = {
    val config = Config(brokerId, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config)
    val replicaManager = new ReplicaManager(config)
    val socketServer1 = new SimpleSocketServer(config.brokerId, config.hostName, config.port, new SimpleKafkaApi(config, replicaManager))
    val controller = new Controller(zookeeperClient, config.brokerId, socketServer1)
    new Server(config, zookeeperClient, controller, socketServer1)
  }
}
