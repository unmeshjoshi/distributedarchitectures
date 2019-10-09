package org.dist.kvstore

import java.util

import scala.jdk.CollectionConverters._

object GossipDigestAck {
  def create(gDigestList: List[GossipDigest], epStateMap: Map[InetAddressAndPort, EndPointState]) = {
    val map: util.Map[String, EndPointState] = new util.HashMap[String, EndPointState]()
    val set = epStateMap.keySet
    for (key <- set) {
      val newKey = s"${key.address.getHostAddress}:${key.port}"
      map.put(newKey, epStateMap.asJava.get(key))
    }
    GossipDigestAck(gDigestList, map.asScala.toMap)
  }
}

case class GossipDigestAck(val gDigestList: List[GossipDigest],
                           val epStateMap: Map[String, EndPointState]) {

  def digestList = if (gDigestList == null) List[GossipDigest]() else gDigestList

  def stateMap() = {
    if (epStateMap == null) {
      new util.HashMap[InetAddressAndPort, EndPointState]().asScala
    } else {
      val map: util.Map[InetAddressAndPort, EndPointState] = new util.HashMap[InetAddressAndPort, EndPointState]()
      val set = epStateMap.keySet
      for (key <- set) {
        val splits = key.split(":")
        map.put(InetAddressAndPort.create(splits(0), splits(1).toInt), epStateMap.asJava.get(key))
      }
      map.asScala
    }
  }

}