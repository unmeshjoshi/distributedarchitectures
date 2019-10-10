package org.dist.simplekafka

import org.dist.queue.common.TopicAndPartition

case class ProduceRequest(topicAndPartition: TopicAndPartition, key:String, message:String)
