package org.dist.queue.api

import org.dist.queue.common.ErrorMapping
import org.dist.queue.utils.ZkUtils.Broker

case class TopicMetadataResponse(topicsMetadata: Seq[TopicMetadata],
                                 val correlationId: Int)

case class TopicMetadata(topic: String, partitionsMetadata: Seq[PartitionMetadata], errorCode: Short = ErrorMapping.NoError)

case class PartitionMetadata(partitionId: Int,
                             val leader: Option[Broker],
                             replicas: Seq[Broker],
                             isr: Seq[Broker] = Seq.empty,
                             errorCode: Short = ErrorMapping.NoError)
