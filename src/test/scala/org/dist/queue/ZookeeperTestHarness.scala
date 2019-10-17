package org.dist.queue

import org.I0Itec.zkclient.ZkClient
import org.dist.queue.utils.ZKStringSerializer
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FunSuite}

object TestZKUtils {
  val zookeeperConnect = "127.0.0.1:2182"
}

trait ZookeeperTestHarness extends FunSuite with BeforeAndAfterEach {
  val zkConnect: String = TestZKUtils.zookeeperConnect
  var zookeeper: EmbeddedZookeeper = null
  var zkClient: ZkClient = null
  val zkConnectionTimeout = 10000
  val zkSessionTimeout = 15000

  override def beforeEach() = {
    zookeeper = new EmbeddedZookeeper(zkConnect)
    zkClient = new ZkClient(zookeeper.connectString, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
  }

  override def afterEach() = {
    zkClient.close()
    zookeeper.shutdown()
  }
}
