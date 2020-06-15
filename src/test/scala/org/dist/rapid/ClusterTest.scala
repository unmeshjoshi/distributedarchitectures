package org.dist.rapid

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.TestUtils
import org.dist.util.Networks
import org.scalatest.FunSuite

class ClusterTest extends FunSuite {

  test("should start 3 node cluster") {
    val address = new Networks().ipv4Address
    val ports = TestUtils.choosePorts(3)
    val peerAddr1 = InetAddressAndPort(address, ports(0))
    val peerAddr2 = InetAddressAndPort(address, ports(1))
    val peerAddr3 = InetAddressAndPort(address, ports(2))

    val cluster = new Cluster(peerAddr1)
    cluster.start()

    val cluster1 = new Cluster(peerAddr2)
    cluster1.join(peerAddr1)

    Thread.sleep(1000000)
  }

}
