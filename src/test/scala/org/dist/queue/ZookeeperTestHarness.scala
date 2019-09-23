package org.dist.queue

import org.I0Itec.zkclient.ZkClient
import org.dist.queue.utils.ZKStringSerializer
import org.scalatest.{BeforeAndAfter, FunSuite}

object TestZKUtils {
  val zookeeperConnect = "127.0.0.1:2182"
}

trait ZookeeperTestHarness extends FunSuite with BeforeAndAfter {
  val zkConnect: String = TestZKUtils.zookeeperConnect
  var zookeeper: EmbeddedZookeeper = null
  var zkClient: ZkClient = null
  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  before {
    zookeeper = new EmbeddedZookeeper(zkConnect)
    zkClient = new ZkClient(zookeeper.connectString, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
  }

  after {
    zkClient.close()
    zookeeper.shutdown()
  }
}
