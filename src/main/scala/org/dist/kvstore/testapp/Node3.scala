package org.dist.kvstore.testapp

import org.dist.kvstore.testapp.Utils.createDbDir
import org.dist.kvstore.{DatabaseConfiguration, InetAddressAndPort, StorageService}
import org.dist.util.Networks

object Node3 extends App {
  val localIpAddress = new Networks().ipv4Address
  private val node1Endpoint = InetAddressAndPort(localIpAddress, 8000)
  val seedConfig = DatabaseConfiguration(Set(node1Endpoint), createDbDir("node3"))

  private val node3Endpoint = InetAddressAndPort(localIpAddress, 8002)
  private val node3ClientEndpoint = InetAddressAndPort(localIpAddress, 9003)
  val node3 = new StorageService(node3ClientEndpoint, node3Endpoint, seedConfig)

  node3.start()
}
