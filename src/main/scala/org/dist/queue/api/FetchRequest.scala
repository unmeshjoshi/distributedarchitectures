package org.dist.queue.api

import org.dist.queue.{ConsumerConfig, TopicAndPartition}

import scala.collection.immutable.Map

case class PartitionFetchInfo(offset: Long, fetchSize: Int)


object FetchRequest {
  val CurrentVersion = 0.shortValue
  val DefaultMaxWait = 0
  val DefaultMinBytes = 0
  val DefaultCorrelationId = 0
}

case class FetchRequest private[queue] (versionId: Short = FetchRequest.CurrentVersion,
                                        val correlationId: Int = FetchRequest.DefaultCorrelationId,
                                        clientId: String = ConsumerConfig.DefaultClientId,
                                        replicaId: Int = Request.OrdinaryConsumerId,
                                        maxWait: Int = FetchRequest.DefaultMaxWait,
                                        minBytes: Int = FetchRequest.DefaultMinBytes,
                                        requestInfo: Map[TopicAndPartition, PartitionFetchInfo]) {
  def isFromFollower = replicaId != Request.OrdinaryConsumerId && replicaId != Request.DebuggingConsumerId

  def isFromOrdinaryConsumer = replicaId == Request.OrdinaryConsumerId

  def isFromLowLevelConsumer = replicaId == Request.DebuggingConsumerId

  def numPartitions = requestInfo.size


}
