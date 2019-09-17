package org.dist.kvstore.testapp

import java.io.File
import java.nio.file.{Path, Paths}

import org.dist.kvstore.{DatabaseConfiguration, InetAddressAndPort, StorageService}
import org.dist.util.Networks

import Utils._

object GossipTestApp extends App {
  val localIpAddress = new Networks().ipv4Address
  private val node1Endpoint = InetAddressAndPort(localIpAddress, 8000)


  val node1 = new StorageService(node1Endpoint, DatabaseConfiguration(Set(node1Endpoint), createDbDir("node1")))

  private val node2Endpoint = InetAddressAndPort(localIpAddress, 8001)
  val node2 = new StorageService(node2Endpoint, DatabaseConfiguration(Set(node1Endpoint), createDbDir("node2")))

  private val node3Endpoint = InetAddressAndPort(localIpAddress, 8002)
  val node3 = new StorageService(node3Endpoint, DatabaseConfiguration(Set(node1Endpoint), createDbDir("node3")))

  node1.start()
  node2.start()
  node3.start()
}
