package org.dist.simplekafka

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks
import org.mockito.{ArgumentMatchers, Mockito}

class ServerTest extends ZookeeperTestHarness {
  test("should register itself to zookeeper on startup") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val client: ZookeeperClient = Mockito.mock(classOf[ZookeeperClient])
    val leaderElector: Controller = Mockito.mock(classOf[Controller])
    var server = new Server(config, client, leaderElector)
    server.startup()
    Mockito.verify(client, Mockito.atLeastOnce()).registerSelf()
  }

  test("should elect controller on startup") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val client: ZookeeperClient = Mockito.mock(classOf[ZookeeperClient])
    val leaderElector: Controller = Mockito.mock(classOf[Controller])
    var server = new Server(config, client, leaderElector)
    server.startup()
    Mockito.verify(client, Mockito.atLeastOnce()).registerSelf()
    Mockito.verify(leaderElector, Mockito.atLeastOnce()).elect()
  }
}
