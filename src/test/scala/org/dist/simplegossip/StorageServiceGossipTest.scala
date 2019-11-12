package org.dist.simplegossip

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.TestUtils
import org.dist.util.Networks
import org.scalatest.FunSuite

import scala.jdk.CollectionConverters._

class StorageServiceGossipTest extends FunSuite {
  test("should gossip state to all the nodes in the cluster") {
    val localIp = new Networks().ipv4Address
    val seed = InetAddressAndPort(localIp, 8080)
    val s1 = new StorageService(seed, seed)
    s1.start()

    val storages = new java.util.ArrayList[StorageService]()
    val basePort = 8081
    val serverCount = 10
    for (i ← 1 to serverCount) {
      val storage = new StorageService(seed, InetAddressAndPort(localIp, basePort + i))
      storage.start()
      storages.add(storage)
    }
    TestUtils.waitUntilTrue(() ⇒ {
      //serverCount + 1 seed
      storages.asScala.toList.map(s ⇒ s.gossiper.endpointStatemap.size() == serverCount + 1).reduce(_ && _)
    }, "Waiting for all the endpoints to be available on all nodes", 15000)

    storages.asScala.foreach(s ⇒ {
      assert(s1.gossiper.endpointStatemap.values().contains(s.gossiper.token))
    })
  }
}
