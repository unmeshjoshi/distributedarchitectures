package org.dist.kvstore.testapp

import java.io.File
import java.nio.file.{Path, Paths}

import org.dist.kvstore.{DatabaseConfiguration, InetAddressAndPort, StorageService}
import org.dist.util.Networks
import Utils._
import org.dist.kvstore.client.Client

object GossipTestApp extends App {
  val localIpAddress = new Networks().ipv4Address
  private val node1Endpoint = InetAddressAndPort(localIpAddress, 8000)
  private val node1ClientEndpoint = InetAddressAndPort(localIpAddress, 9000)


  val node1 = new StorageService(node1ClientEndpoint, node1Endpoint, DatabaseConfiguration(Set(node1Endpoint), createDbDir("node1")))

  private val node2Endpoint = InetAddressAndPort(localIpAddress, 8001)
  private val node2ClientEndpoint = InetAddressAndPort(localIpAddress, 9001)
  val node2 = new StorageService(node2ClientEndpoint, node2Endpoint, DatabaseConfiguration(Set(node1Endpoint), createDbDir("node2")))

  private val node3Endpoint = InetAddressAndPort(localIpAddress, 8002)
  private val node3ClientEndpoint = InetAddressAndPort(localIpAddress, 9003)
  val node3 = new StorageService(node3ClientEndpoint, node3Endpoint, DatabaseConfiguration(Set(node1Endpoint), createDbDir("node3")))

  node1.start()
  node2.start()
  node3.start()

  println("Waiting for gossip to settle")
  Thread.sleep(5000)

  val client = new Client(node1ClientEndpoint)
  client.put("table1", "key1", "value1")


}
