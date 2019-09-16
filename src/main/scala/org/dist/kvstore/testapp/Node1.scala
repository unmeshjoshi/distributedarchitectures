package org.dist.kvstore.testapp

import org.dist.kvstore.{DatabaseConfiguration, InetAddressAndPort, StorageService}
import org.dist.util.Networks

object Node1 extends App {
  val localIpAddress = new Networks().ipv4Address
  private val node1Endpoint = InetAddressAndPort(localIpAddress, 8000)
  val seedConfig = DatabaseConfiguration(Set(node1Endpoint))

  val node1 = new StorageService(node1Endpoint, seedConfig)

  node1.start()
}
