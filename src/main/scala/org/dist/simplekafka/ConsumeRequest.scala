package org.dist.simplekafka

import org.dist.queue.common.TopicAndPartition

case class ConsumeRequest(topicAndPartition: TopicAndPartition, offset:Int = 0)
