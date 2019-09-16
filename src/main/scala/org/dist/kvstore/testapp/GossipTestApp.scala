package org.dist.kvstore.testapp

import org.dist.kvstore.{DatabaseConfiguration, InetAddressAndPort, StorageService}
import org.dist.util.Networks

object GossipTestApp extends App {
  val localIpAddress = new Networks().ipv4Address
  private val node1Endpoint = InetAddressAndPort(localIpAddress, 8000)
  val seedConfig = DatabaseConfiguration(Set(node1Endpoint))

  val node1 = new StorageService(node1Endpoint, seedConfig)

  private val node2Endpoint = InetAddressAndPort(localIpAddress, 8001)
  val node2 = new StorageService(node2Endpoint, seedConfig)

  private val node3Endpoint = InetAddressAndPort(localIpAddress, 8002)
  val node3 = new StorageService(node3Endpoint, seedConfig)

  node1.start()
  node2.start()
  node3.start()
}
