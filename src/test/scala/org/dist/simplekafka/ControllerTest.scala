package org.dist.simplekafka

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks
import org.mockito.{ArgumentMatchers, Mockito}

class ControllerTest extends ZookeeperTestHarness {

  test("Should elect first server as controller and register for topic changes") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClient = Mockito.mock(classOf[ZookeeperClient])
    val controller = new Controller(zookeeperClient, config.brokerId)
    controller.elect()

    Mockito.when(zookeeperClient.subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())).thenReturn(None)

    Mockito.verify(zookeeperClient, Mockito.atLeastOnce()).subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())
  }

  test("Should not register for topic changes if controller already exists") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient1: ZookeeperClient = Mockito.mock(classOf[ZookeeperClient])

    val controller1 = new Controller(zookeeperClient1, config.brokerId)
    Mockito.doNothing().when(zookeeperClient1).tryCreatingControllerPath("1")

    controller1.elect()


    val zookeeperClient2: ZookeeperClient = Mockito.mock(classOf[ZookeeperClient])
    Mockito.doThrow(new ControllerExistsException("1")).when(zookeeperClient2).tryCreatingControllerPath("1")

    val controller2 = new Controller(zookeeperClient2, config.brokerId)
    controller2.elect()

    Mockito.verify(zookeeperClient1, Mockito.atLeastOnce()).subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())
    Mockito.verify(zookeeperClient2, Mockito.atMost(0)).subscribeTopicChangeListener(ArgumentMatchers.any[IZkChildListener]())
  }
}
