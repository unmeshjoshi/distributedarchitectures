package org.dist.kvstore

import java.util

import scala.jdk.CollectionConverters._

object GossipDigestAck2 {
  def create(epStateMap: Map[InetAddressAndPort, EndPointState]):GossipDigestAck2 = {
    val map: util.Map[String, EndPointState] = new util.HashMap[String, EndPointState]()
    val set = epStateMap.keySet
    for (key <- set) {
      val newKey = s"${key.address.getHostAddress}:${key.port}"
      map.put(newKey, epStateMap.asJava.get(key))
    }
    GossipDigestAck2(map)
  }
}
case class GossipDigestAck2(val epStateMap: util.Map[String, EndPointState]) {
  def stateMap(): util.Map[InetAddressAndPort, EndPointState] = {
    if (epStateMap == null) {
      new util.HashMap[InetAddressAndPort, EndPointState]()
    } else {
      val map: util.Map[InetAddressAndPort, EndPointState] = new util.HashMap[InetAddressAndPort, EndPointState]()
      val set = epStateMap.keySet.asScala
      for (key <- set) {
        val splits = key.split(":")
        map.put(InetAddressAndPort.create(splits(0), splits(1).toInt), epStateMap.get(key))
      }
      map
    }
  }
}
