package org.dist.simplekafka

import java.util

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks
import scala.jdk.CollectionConverters._

class TestSocketServer(config: Config) extends SimpleSocketServer(config.brokerId, config.hostName, config.port, null) {
  var messages = new util.ArrayList[RequestOrResponse]()
  var toAddresses = new util.ArrayList[InetAddressAndPort]()

  override def sendReceiveTcp(message: RequestOrResponse, to: InetAddressAndPort): RequestOrResponse = {
    this.messages.add(message)
    this.toAddresses.add(to)
    RequestOrResponse(message.requestId, "", message.correlationId)
  }
}

class ControllerZookeeperTest extends ZookeeperTestHarness {
  test("should send LeaderAndFollower requests to all leader and follower brokers for given topicandpartition") {
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config1)
    zookeeperClient.registerBroker(Broker(config1.brokerId, config1.hostName, config1.port))

    val socketServer1 = new TestSocketServer(config1)
    val controller = new Controller(zookeeperClient, config1.brokerId, socketServer1)
    controller.startup()

    val config2 = Config(2, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    zookeeperClient.registerBroker(Broker(config2.brokerId, config2.hostName, config2.port))

    val config3 = Config(3, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    zookeeperClient.registerBroker(Broker(config3.brokerId, config3.hostName, config3.port))


    TestUtils.waitUntilTrue(() => {
      controller.liveBrokers.size == 3
    }, "Waiting for all brokers to get added", 1000)

    assert(controller.liveBrokers.size == 3)

    val createCommandTest = new CreateTopicCommand(zookeeperClient)
    createCommandTest.createTopic("topic1", 2, 1)

    TestUtils.waitUntilTrue(() ⇒ {
      socketServer1.messages.size == 5 && socketServer1.toAddresses.asScala.toSet.size == 3
    }, "waiting for leader and replica requests handled in all brokers", 2000)

    socketServer1.messages.asScala.filter(m⇒ m.requestId == RequestKeys.LeaderAndIsrKey).toList.foreach(message ⇒ {
      assert(message.requestId == RequestKeys.LeaderAndIsrKey)
    })

    assert(socketServer1.toAddresses.asScala.toSet == Set(InetAddressAndPort.create(config1.hostName, config1.port),
                                                          InetAddressAndPort.create(config2.hostName, config2.port),
                                                          InetAddressAndPort.create(config3.hostName, config3.port)))

  }

  test("should send UpdateMetadata requests to all brokers") {
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config1)
    zookeeperClient.registerBroker(Broker(config1.brokerId, config1.hostName, config1.port))

    val socketServer1 = new TestSocketServer(config1)
    val controller = new Controller(zookeeperClient, config1.brokerId, socketServer1)
    controller.startup()

    val config2 = Config(2, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    zookeeperClient.registerBroker(Broker(config2.brokerId, config2.hostName, config2.port))

    val config3 = Config(3, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    zookeeperClient.registerBroker(Broker(config3.brokerId, config3.hostName, config3.port))


    TestUtils.waitUntilTrue(() => {
      controller.liveBrokers.size == 3
    }, "Waiting for all brokers to get added", 1000)

    assert(controller.liveBrokers.size == 3)

    val createCommandTest = new CreateTopicCommand(zookeeperClient)
    createCommandTest.createTopic("topic1", 2, 3)

    TestUtils.waitUntilTrue(() ⇒ {
      socketServer1.messages.size == 6 && socketServer1.toAddresses.asScala.toSet.size == 3
    }, "waiting for leader and replica requests handled in all brokers", 2000)

    socketServer1.messages.asScala.filter(m⇒ m.requestId == RequestKeys.UpdateMetadataKey).toList.foreach(message ⇒ {
      assert(message.requestId == RequestKeys.UpdateMetadataKey)
    })

    assert(socketServer1.toAddresses.asScala.toSet == Set(InetAddressAndPort.create(config1.hostName, config1.port),
      InetAddressAndPort.create(config2.hostName, config2.port),
      InetAddressAndPort.create(config3.hostName, config3.port)))
 }
}
