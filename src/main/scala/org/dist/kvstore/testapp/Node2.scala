package org.dist.kvstore.testapp

import org.dist.kvstore.testapp.GossipTestApp.localIpAddress
import org.dist.kvstore.testapp.Utils.createDbDir
import org.dist.kvstore.{DatabaseConfiguration, InetAddressAndPort, StorageService}
import org.dist.util.Networks

object Node2 extends App {

  val localIpAddress = new Networks().ipv4Address

  private val node1Endpoint = InetAddressAndPort(localIpAddress, 8000)
  val seedConfig = DatabaseConfiguration(Set(node1Endpoint), createDbDir("node2"))

  private val node2Endpoint = InetAddressAndPort(localIpAddress, 8001)
  private val node2ClientEndpoint = InetAddressAndPort(localIpAddress, 9001)

  val node2 = new StorageService(node2ClientEndpoint, node2Endpoint, seedConfig)

  node2.start()


}
