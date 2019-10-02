package org.dist.queue

import java.net.InetSocketAddress

import org.apache.zookeeper.server.{NIOServerCnxn, NIOServerCnxnFactory, ZooKeeperServer}
import org.dist.queue.utils.Utils


class EmbeddedZookeeper(val connectString: String) {
  val snapshotDir = TestUtils.tempDir()
  val logDir = TestUtils.tempDir()
  val tickTime = 500
  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime)
  val port = connectString.split(":")(1).toInt
  val factory = new NIOServerCnxnFactory()
  factory.configure(new InetSocketAddress("127.0.0.1", port), 60)
  factory.startup(zookeeper)

  def shutdown() {
    factory.shutdown()
    Utils.rm(logDir)
    Utils.rm(snapshotDir)
  }

}