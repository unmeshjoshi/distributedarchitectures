package org.dist.kvstore

import java.util
import scala.collection.JavaConverters._

case class DatabaseConfiguration(seeds:Set[InetAddressAndPort]) {
  def getClusterName() = "TestCluster"


  def nonLocalSeeds(localEndpoint:InetAddressAndPort) = {
    seeds.filter(address ⇒ address != localEndpoint).toList.asJava
  }

}
