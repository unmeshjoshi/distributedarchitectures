package org.dist.queue.api

case class LeaderAndIsrResponse(val correlationId: Int,
                                responseMap: Map[(String, Int), Short],
                                errorCode: Short = 0)
