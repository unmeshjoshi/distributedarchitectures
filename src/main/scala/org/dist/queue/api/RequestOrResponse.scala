package org.dist.queue.api

import org.dist.kvstore.JsonSerDes

object Request {
  val FollowerReplicaFetcherId: Int = 0
  val OrdinaryConsumerId: Int = -1
  val DebuggingConsumerId: Int = -2
}


object RequestKeys {
  val ProduceKey: Short = 0
  val FetchKey: Short = 1
  val OffsetsKey: Short = 2
  val GetMetadataKey: Short = 3
  val LeaderAndIsrKey: Short = 4
  val StopReplicaKey: Short = 5
  val UpdateMetadataKey: Short = 6
  val ControlledShutdownKey: Short = 7
  val FindCoordinatorKey:Short = 8
}

case class RequestOrResponse(val requestId: Short, val messageBodyJson: String, val correlationId: Int) {
  def serialize(): String = {
    JsonSerDes.serialize(this)
  }
}