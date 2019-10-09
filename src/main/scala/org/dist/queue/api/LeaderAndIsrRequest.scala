package org.dist.queue.api

import java.util

import org.dist.queue.controller.PartitionStateInfo
import org.dist.queue.utils.ZkUtils.Broker

import scala.jdk.CollectionConverters._

object LeaderAndIsrRequest {
  val CurrentVersion = 0.shortValue
  val IsInit: Boolean = true
  val NotInit: Boolean = false
  val DefaultAckTimeout: Int = 1000

  def apply(partitionStateInfos: Map[(String, Int), PartitionStateInfo],
            leaders: Set[Broker], controllerId: Int,
           controllerEpoch: Int, correlationId: Int, clientId: String) = {

    new LeaderAndIsrRequest(LeaderAndIsrRequest.CurrentVersion, correlationId, clientId,
      controllerId, controllerEpoch, convertToStringKeyMap(partitionStateInfos), leaders)
  }


  def convertToStringKeyMap(partitionStateInfos: Map[(String, Int), PartitionStateInfo]):Map[String, PartitionStateInfo] = {
    val map = new util.HashMap[String, PartitionStateInfo]()
    val keys = partitionStateInfos.keySet
    for(key <- keys) {
      val strKey = s"${key._1}:${key._2}"
      map.put(strKey, partitionStateInfos(key))
    }
    map.asScala.toMap
  }

}

case class LeaderAndIsrRequest(versionId: Short,
                               val correlationId: Int,
                               clientId: String,
                               controllerId: Int,
                               controllerEpoch: Int,
                               partitionStateInfos: Map[String, PartitionStateInfo], // cant deserialize map with tuple (String, Int) key so adding helper method
                               leaders: Set[Broker]) {




  def partitionStateInfoMap = {


    if (partitionStateInfos == null) {
      Map[(String, Int), PartitionStateInfo]()
    } else {
      val map = new util.HashMap[(String, Int), PartitionStateInfo]()
      val set = partitionStateInfos.keySet
      for (key <- set) {
        val splits: Array[String] = key.split(":")
        val tuple = (splits(0), splits(1).toInt)
        map.put(tuple, partitionStateInfos(key))
      }
      map.asScala.toMap
    }
  }
}