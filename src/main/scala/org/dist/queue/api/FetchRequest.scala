package org.dist.queue.api

import java.util

import org.dist.queue.client.consumer.ConsumerConfig
import org.dist.queue.common.TopicAndPartition

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Map

case class PartitionFetchInfo(offset: Long, fetchSize: Int)


object FetchRequest {
  val CurrentVersion = 0.shortValue
  val DefaultMaxWait = 0
  val DefaultMinBytes = 0
  val DefaultCorrelationId = 0

  def apply(correlationId: Int,
            clientId: String = ConsumerConfig.DefaultClientId,
            replicaId: Int = Request.OrdinaryConsumerId,
            maxWait: Int = FetchRequest.DefaultMaxWait,
            minBytes: Int = FetchRequest.DefaultMinBytes,
            requestInfo: Map[TopicAndPartition, PartitionFetchInfo]) = {

    new FetchRequest(FetchRequest.CurrentVersion, correlationId, clientId, replicaId, maxWait, minBytes, convertToStringKeyMap(requestInfo))
  }


  def convertToStringKeyMap(data: Map[TopicAndPartition, PartitionFetchInfo]):Map[String, PartitionFetchInfo] = {
    val map = new util.HashMap[String, PartitionFetchInfo]()
    for(key <- data.keySet) {
      val strKey = s"${key.topic}:${key.partition}"
      map.put(strKey, data(key))
    }
    map.asScala.toMap
  }
}

case class FetchRequest(versionId: Short,
                                        val correlationId: Int,
                                        clientId: String,
                                        replicaId: Int,
                                        maxWait: Int,
                                        minBytes: Int,
                                        requestInfo: Map[String, PartitionFetchInfo]) {
  def isFromFollower = replicaId != Request.OrdinaryConsumerId && replicaId != Request.DebuggingConsumerId

  def isFromOrdinaryConsumer = replicaId == Request.OrdinaryConsumerId

  def isFromLowLevelConsumer = replicaId == Request.DebuggingConsumerId

  def numPartitions = requestInfo.size


  def requestInfoAsMap = {
       if (requestInfo == null) {
        Map[TopicAndPartition, PartitionFetchInfo]()
      } else {
        val map = new util.HashMap[TopicAndPartition, PartitionFetchInfo]()
        val set = requestInfo.keySet
        for (key <- set) {
          val splits: Array[String] = key.split(":")
          val tuple = TopicAndPartition(splits(0), splits(1).toInt)
          map.put(tuple, requestInfo(key))
        }
        map.asScala.toMap
      }
  }


}
