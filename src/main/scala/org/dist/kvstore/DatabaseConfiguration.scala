package org.dist.kvstore

import java.util
import scala.jdk.CollectionConverters._

case class DatabaseConfiguration(seeds:Set[InetAddressAndPort], systemDir:String = System.getProperty(("java.io.tmpdir"))) {
  def getSystemDir(): String = systemDir

  def getClusterName() = "TestCluster"


  def nonLocalSeeds(localEndpoint:InetAddressAndPort) = {
    seeds.filter(address => address != localEndpoint).toList.asJava
  }

}
