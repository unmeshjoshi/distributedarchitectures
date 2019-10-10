package org.dist.simplekafka

import org.dist.queue.common.TopicAndPartition

case class TopicMetadataResponse(topicPartitions:Map[TopicAndPartition, PartitionInfo])
