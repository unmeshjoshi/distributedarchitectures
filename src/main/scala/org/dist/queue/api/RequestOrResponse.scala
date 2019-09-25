package org.dist.queue.api

import org.dist.queue.PartitionStateInfo
import org.dist.queue.utils.ZkUtils.Broker
import java.util

object RequestKeys {
  val ProduceKey: Short = 0
  val FetchKey: Short = 1
  val OffsetsKey: Short = 2
  val MetadataKey: Short = 3
  val LeaderAndIsrKey: Short = 4
  val StopReplicaKey: Short = 5
  val UpdateMetadataKey: Short = 6
  val ControlledShutdownKey: Short = 7
}

case class RequestOrResponse(val requestId: Short, val messageBody: Any, val correlationId: Int)

object LeaderAndIsrRequest {

}

case class LeaderAndIsrRequest(versionId: Short,
                               clientId: String,
                               controllerId: Int,
                               controllerEpoch: Int,
                               partitionStateInfos: Map[String, PartitionStateInfo], // cant deserialize map with tuple (String, Int) key so adding helper method
                               leaders: Set[Broker]) {

  def partitionStateInfoMap = {
    import scala.collection.JavaConverters._

    if (partitionStateInfos == null) {
      Map[(String, Int), PartitionStateInfo]()
    } else {
      val map = new util.HashMap[(String, Int), PartitionStateInfo]()
      val set = partitionStateInfos.keySet
      for (key ← set) {
        val splits: Array[String] = key.split(":")
        val tuple = (splits(0), splits(1).toInt)
        map.put(tuple, partitionStateInfos(key))
      }
      map.asScala.toMap
    }
  }
}
