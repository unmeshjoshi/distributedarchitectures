package org.dist.queue.api

import org.dist.queue.common.TopicAndPartition

case class ProducerResponse(val correlationId: Int,
                            status: Map[TopicAndPartition, ProducerResponseStatus])

case class ProducerResponseStatus(error: Short, offset: Long)

