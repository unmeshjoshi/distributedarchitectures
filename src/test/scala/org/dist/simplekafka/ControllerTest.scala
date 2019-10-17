package org.dist.simplekafka

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks
import org.mockito.{ArgumentMatchers, Mockito}

class ControllerTest extends ZookeeperTestHarness {

  test("should register for broker changes") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClient = Mockito.mock(classOf[ZookeeperClient])
    val socketServer = Mockito.mock(classOf[SimpleSocketServer])
    val controller = new Controller(zookeeperClient, config.brokerId, socketServer)
    Mockito.when(zookeeperClient.subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())).thenReturn(None)
    Mockito.when(zookeeperClient.subscribeBrokerChangeListener(ArgumentMatchers.any[IZkChildListener]())).thenReturn(None)
    Mockito.when(zookeeperClient.getAllBrokers()).thenReturn(Set(Broker(1, "10.10.10.10", 9000)))

    controller.startup()

    Mockito.verify(zookeeperClient, Mockito.atLeastOnce()).subscribeBrokerChangeListener(ArgumentMatchers.any[IZkChildListener]())
  }

  test("Should elect first server as controller and get all live brokers") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClient = Mockito.mock(classOf[ZookeeperClient])
    val socketServer = Mockito.mock(classOf[SimpleSocketServer])
    val controller = new Controller(zookeeperClient, config.brokerId, socketServer)

    Mockito.when(zookeeperClient.subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())).thenReturn(None)
    Mockito.when(zookeeperClient.subscribeBrokerChangeListener(ArgumentMatchers.any[IZkChildListener]())).thenReturn(None)

    Mockito.when(zookeeperClient.getAllBrokers()).thenReturn(Set(Broker(1, "10.10.10.10", 9000)))

    controller.startup()


    Mockito.verify(zookeeperClient, Mockito.atLeastOnce()).subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())
    Mockito.verify(zookeeperClient, Mockito.atLeastOnce()).subscribeBrokerChangeListener(ArgumentMatchers.any[IZkChildListener]())
    assert(controller.liveBrokers == Set(Broker(1, "10.10.10.10", 9000)))
  }

  test("Should elect first server as controller and register for topic changes") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClient = Mockito.mock(classOf[ZookeeperClient])
    val socketServer = Mockito.mock(classOf[SimpleSocketServer])
    val controller = new Controller(zookeeperClient, config.brokerId, socketServer)

    Mockito.when(zookeeperClient.subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())).thenReturn(None)
    Mockito.when(zookeeperClient.subscribeBrokerChangeListener(ArgumentMatchers.any[IZkChildListener]())).thenReturn(None)

    Mockito.when(zookeeperClient.getAllBrokers()).thenReturn(Set(Broker(1, "10.10.10.10", 9000)))

    controller.startup()

    Mockito.verify(zookeeperClient, Mockito.atLeastOnce()).subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())
  }

  test("Should not register for topic changes if controller already exists") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient1: ZookeeperClient = Mockito.mock(classOf[ZookeeperClient])

    val socketServer = Mockito.mock(classOf[SimpleSocketServer])
    val controller1 = new Controller(zookeeperClient1, config.brokerId, socketServer)

    Mockito.doNothing().when(zookeeperClient1).tryCreatingControllerPath("1")
    Mockito.when(zookeeperClient1.getAllBrokers()).thenReturn(Set(Broker(1, "10.10.10.10", 9000)))

    controller1.startup()


    val zookeeperClient2: ZookeeperClient = Mockito.mock(classOf[ZookeeperClient])
    Mockito.doThrow(new ControllerExistsException("1")).when(zookeeperClient2).tryCreatingControllerPath("1")

    val controller2 = new Controller(zookeeperClient2, config.brokerId, socketServer)
    controller2.startup()

    Mockito.verify(zookeeperClient1, Mockito.atLeastOnce()).subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())
    Mockito.verify(zookeeperClient1, Mockito.atLeastOnce()).subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())

    Mockito.verify(zookeeperClient2, Mockito.atMost(0)).subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())
    Mockito.verify(zookeeperClient2, Mockito.atMost(0)).subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())
  }
}
