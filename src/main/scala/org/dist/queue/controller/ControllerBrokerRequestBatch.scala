package org.dist.queue.controller

import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{LeaderAndIsrRequest, RequestKeys, RequestOrResponse, UpdateMetadataRequest}
import org.dist.queue.common.{Logging, TopicAndPartition}

import scala.collection.{Seq, Set, mutable}

class ControllerBrokerRequestBatch(controllerContext: ControllerContext, sendRequest: (Int, RequestOrResponse, (RequestOrResponse) => Unit) => Unit,
                                   controllerId: Int, clientId: String) extends Logging {
  val leaderAndIsrRequestMap = new mutable.HashMap[Int, mutable.HashMap[(String, Int), PartitionStateInfo]]
  val stopReplicaRequestMap = new mutable.HashMap[Int, Seq[(String, Int)]]
  val stopAndDeleteReplicaRequestMap = new mutable.HashMap[Int, Seq[(String, Int)]]
  val updateMetadataRequestMap = new mutable.HashMap[Int, mutable.HashMap[TopicAndPartition, PartitionStateInfo]]

  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicas: Seq[Int]) = {
    brokerIds.foreach { brokerId =>
      leaderAndIsrRequestMap.getOrElseUpdate(brokerId, new mutable.HashMap[(String, Int), PartitionStateInfo])
      leaderAndIsrRequestMap(brokerId).put((topic, partition),
        PartitionStateInfo(leaderIsrAndControllerEpoch, replicas.toSet))
    }
    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(TopicAndPartition(topic, partition)))
  }

  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: scala.collection.Set[TopicAndPartition] = Set.empty[TopicAndPartition]) {
    val partitionList =
      if (partitions.isEmpty) {
        controllerContext.partitionLeadershipInfo.keySet
      } else {
        partitions
      }
    partitionList.foreach { partition =>
      val leaderIsrAndControllerEpochOpt = controllerContext.partitionLeadershipInfo.get(partition)
      leaderIsrAndControllerEpochOpt match {
        case Some(leaderIsrAndControllerEpoch) =>
          val replicas = controllerContext.partitionReplicaAssignment(partition).toSet
          val partitionStateInfo = PartitionStateInfo(leaderIsrAndControllerEpoch, replicas)
          brokerIds.foreach { brokerId =>
            updateMetadataRequestMap.getOrElseUpdate(brokerId, new mutable.HashMap[TopicAndPartition, PartitionStateInfo])
            updateMetadataRequestMap(brokerId).put(partition, partitionStateInfo)
          }
        case None =>
          info("Leader not assigned yet for partition %s. Skip sending udpate metadata request".format(partition))
      }
    }
  }


  def sendRequestsToBrokers(controllerEpoch: Int, correlationId: Int): Unit = {
    //send leaderandisr requests
    //send update metadata requests
    //send stop replica requests
    leaderAndIsrRequestMap.foreach { m =>
      val broker = m._1
      val partitionStateInfos: Map[(String, Int), PartitionStateInfo] = m._2.toMap
      val func = (tuple: ((String, Int), PartitionStateInfo)) => tuple._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader
      val leaderIds = partitionStateInfos.map(func).toSet
      val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id))
      val leaderAndIsrRequest = LeaderAndIsrRequest(partitionStateInfos,
        leaders, controllerId, controllerEpoch, correlationId, clientId)
      for (p <- partitionStateInfos) {
        val typeOfRequest = if (broker == p._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader) "become-leader" else "become-follower"
        trace(("Controller %d epoch %d sending %s LeaderAndIsr request with correlationId %d to broker %d " +
          "for partition [%s,%d]").format(controllerId, controllerEpoch, typeOfRequest, correlationId, broker,
          p._1._1, p._1._2))
      }
      sendRequest(broker, RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndIsrRequest), correlationId), null)
    }
    leaderAndIsrRequestMap.clear()
    updateMetadataRequestMap.foreach { m =>
      val broker = m._1
      val partitionStateInfos = m._2.toMap
      val updateMetadataRequest = UpdateMetadataRequest(controllerId, controllerEpoch, correlationId, clientId,
        partitionStateInfos, controllerContext.liveOrShuttingDownBrokers)
      partitionStateInfos.foreach(p => trace(("Controller %d epoch %d sending UpdateMetadata request with " +
        "correlationId %d to broker %d for partition %s").format(controllerId, controllerEpoch, correlationId, broker, p._1)))
      sendRequest(broker, RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest), correlationId), null)
    }
    updateMetadataRequestMap.clear()
  }

  def newBatch() = {
    // raise error if the previous batch is not empty
    if (leaderAndIsrRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        "a new one. Some LeaderAndIsr state changes %s might be lost ".format(leaderAndIsrRequestMap.toString()))
    if (stopReplicaRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some StopReplica state changes %s might be lost ".format(stopReplicaRequestMap.toString()))
    if (updateMetadataRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some UpdateMetadata state changes %s might be lost ".format(updateMetadataRequestMap.toString()))
    if (stopAndDeleteReplicaRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some StopReplica with delete state changes %s might be lost ".format(stopAndDeleteReplicaRequestMap.toString()))

    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    updateMetadataRequestMap.clear()
    stopAndDeleteReplicaRequestMap.clear()
  }

}
