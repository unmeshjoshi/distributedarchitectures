package org.dist.simplegossip

import java.net.InetAddress

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.TestUtils
import org.dist.util.Networks
import org.scalatest.FunSuite

class StorageServiceTest extends FunSuite {

  test("should gossip state to all the nodes in the cluster") {
    val localIp = new Networks().ipv4Address
    val seed = InetAddressAndPort(localIp, 8080)
    val s1 = new StorageService(seed, seed)
    val s2 = new StorageService(seed, InetAddressAndPort(localIp, 8081))
    val s3 = new StorageService(seed, InetAddressAndPort(localIp, 8082))

    s1.start()
    s2.start()
    s3.start()

    TestUtils.waitUntilTrue(()⇒{
      s1.gossiper.endpointStatemap.size() == 3 &&
        s2.gossiper.endpointStatemap.size() == 3 &&
      s3.gossiper.endpointStatemap.size() == 3
    }, "Waiting for all the endpoints to be available on all nodes")
  }
}
