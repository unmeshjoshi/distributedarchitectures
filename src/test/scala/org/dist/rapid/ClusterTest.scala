package org.dist.rapid

import java.util

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.TestUtils
import org.dist.util.Networks
import org.scalatest.FunSuite

import scala.jdk.CollectionConverters._

class ClusterTest extends FunSuite {


  def assertSame(view: MembershipView, view1: MembershipView): Boolean = {
    view.endpoints.size() == view1.endpoints.size() && 
    view.endpoints.asScala.map(address => {
      view1.endpoints.contains(address)
    }).reduce(_ && _)
  }

  test("should start 3 node cluster") {
    val address = new Networks().ipv4Address
    val ports = TestUtils.choosePorts(4)
    val peerAddr1 = InetAddressAndPort(address, ports(0))
    val peerAddr2 = InetAddressAndPort(address, ports(1))

    val seed = new Cluster(peerAddr1)
    seed.start()

    val server1 = new Cluster(peerAddr2)
    server1.join(peerAddr1)

    val expectedView = new util.ArrayList[InetAddressAndPort]()
    expectedView.add(peerAddr1)
    expectedView.add(peerAddr2)

    assertSame(seed.membershipService.view, MembershipView(expectedView))
    assertSame(seed.membershipService.view, server1.membershipService.view)

    val peerAddr3 = InetAddressAndPort(address, ports(2))

    val server2 = new Cluster(peerAddr3)
    server2.join(peerAddr1)

    expectedView.add(peerAddr3)
    assertSame(seed.membershipService.view, new MembershipView(expectedView))
    assertSame(seed.membershipService.view, server1.membershipService.view)
    assertSame(seed.membershipService.view, server2.membershipService.view)

    val peerAddr4 = InetAddressAndPort(address, ports(3))

    val server3 = new Cluster(peerAddr4)
    server3.join(peerAddr1)

    expectedView.add(peerAddr4)
    TestUtils.waitUntilTrue(()=> {
      assertSame(seed.membershipService.view, new MembershipView(expectedView)) &&
      assertSame(seed.membershipService.view, server2.membershipService.view) &&
      assertSame(seed.membershipService.view, server3.membershipService.view) &&
      assertSame(seed.membershipService.view, server1.membershipService.view)
    }, "Waiting for all the servers to agree on a view", 1000, 100)
  }

}
