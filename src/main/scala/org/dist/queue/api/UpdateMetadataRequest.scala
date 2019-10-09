package org.dist.queue.api

import java.util

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.controller.PartitionStateInfo
import org.dist.queue.utils.ZkUtils.Broker

import scala.jdk.CollectionConverters._

object UpdateMetadataRequest {
  val CurrentVersion = 0.shortValue
  val IsInit: Boolean = true
  val NotInit: Boolean = false
  val DefaultAckTimeout: Int = 1000

  def apply(controllerId: Int, controllerEpoch: Int, correlationId: Int, clientId: String,
           partitionStateInfos: Map[TopicAndPartition, PartitionStateInfo], aliveBrokers: Set[Broker]) = {
    new UpdateMetadataRequest(UpdateMetadataRequest.CurrentVersion, correlationId, clientId,
      controllerId, controllerEpoch, convertToStringKeyMap(partitionStateInfos), aliveBrokers)
  }

  def convertToStringKeyMap(partitionStateInfos: Map[TopicAndPartition, PartitionStateInfo]):Map[String, PartitionStateInfo] = {
    val map = new util.HashMap[String, PartitionStateInfo]()
    val keys = partitionStateInfos.keySet
    for(key <- keys) {
      val strKey = s"${key.topic}:${key.partition}"
      map.put(strKey, partitionStateInfos(key))
    }
    map.asScala.toMap
  }


}

case class UpdateMetadataRequest (versionId: Short,
                                  val correlationId: Int,
                                  clientId: String,
                                  controllerId: Int,
                                  controllerEpoch: Int,
                                  partitionStateInfos: Map[String, PartitionStateInfo],
                                  aliveBrokers: Set[Broker]) {

  def partitionStateInfoMap = {
    if (partitionStateInfos == null) {
      Map[TopicAndPartition, PartitionStateInfo]()
    } else {
      val map = new util.HashMap[TopicAndPartition, PartitionStateInfo]()
      val set = partitionStateInfos.keySet
      for (key <- set) {
        val splits: Array[String] = key.split(":")
        val topicPartition = TopicAndPartition(splits(0), splits(1).toInt)
        map.put(topicPartition, partitionStateInfos(key))
      }
      map.asScala.toMap
    }

  }
}