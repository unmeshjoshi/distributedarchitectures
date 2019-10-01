package org.dist.kvstore.testapp

import org.dist.kvstore.{DatabaseConfiguration, InetAddressAndPort, StorageService}
import org.dist.util.Networks
import Utils._
import org.dist.kvstore.testapp.GossipTestApp.localIpAddress

object Node1 extends App {
  val localIpAddress = new Networks().ipv4Address
  private val node1Endpoint = InetAddressAndPort(localIpAddress, 8000)
  private val node1ClientEndpoint = InetAddressAndPort(localIpAddress, 9000)

  val seedConfig = DatabaseConfiguration(Set(node1Endpoint), createDbDir("node1"))

  val node1 = new StorageService(node1ClientEndpoint, node1Endpoint, seedConfig)

  node1.start()
}
