package org.dist.queue

import org.I0Itec.zkclient.ZkClient
import org.dist.kvstore.JsonSerDes
import org.dist.queue.api._
import org.dist.queue.utils.ZkUtils.Broker

import scala.collection.mutable


class KafkaApis(val replicaManager: ReplicaManager,
                val zkClient: ZkClient,
                brokerId: Int,
                val controller: Controller) extends Logging {
  /* following 3 data structures are updated by the update metadata request
   * and is queried by the topic metadata request. */
  var leaderCache: mutable.Map[TopicAndPartition, PartitionStateInfo] =
    new mutable.HashMap[TopicAndPartition, PartitionStateInfo]()
  private val aliveBrokers: mutable.Map[Int, Broker] = new mutable.HashMap[Int, Broker]()
  private val partitionMetadataLock = new Object

  def handle(req: RequestOrResponse): RequestOrResponse = {
    val request: RequestOrResponse = req
    request.requestId match {
      case RequestKeys.UpdateMetadataKey ⇒ {
        println(s"Handling UpdateMetadataRequest ${request.messageBodyJson}")
        val message = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[UpdateMetadataRequest])
        val response = handleUpdateMetadataRequest(message)
        RequestOrResponse(0, JsonSerDes.serialize(response), request.correlationId)

      }
      case RequestKeys.LeaderAndIsrKey ⇒ {
        println(s"Handling LeaderAndIsrRequest ${request.messageBodyJson}" )
        val leaderAndIsrRequest: LeaderAndIsrRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[LeaderAndIsrRequest])
        replicaManager.becomeLeaderOrFollower(leaderAndIsrRequest)
        RequestOrResponse(0, JsonSerDes.serialize(LeaderAndIsrResponse(leaderAndIsrRequest.controllerId, Map(), 0)), leaderAndIsrRequest.correlationId)
      }
    }


  }

  def handleUpdateMetadataRequest(updateMetadataRequest: UpdateMetadataRequest) {
    if (updateMetadataRequest.controllerEpoch < replicaManager.controllerEpoch) {
      val stateControllerEpochErrorMessage = ("Broker %d received update metadata request with correlation id %d from an " +
        "old controller %d with epoch %d. Latest known controller epoch is %d").format(brokerId,
        updateMetadataRequest.correlationId, updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch,
        replicaManager.controllerEpoch)
      warn(stateControllerEpochErrorMessage)
      throw new RuntimeException(stateControllerEpochErrorMessage)
    }
    partitionMetadataLock synchronized {
      replicaManager.controllerEpoch = updateMetadataRequest.controllerEpoch
      // cache the list of alive brokers in the cluster
      updateMetadataRequest.aliveBrokers.foreach(b => aliveBrokers.put(b.id, b))
      updateMetadataRequest.partitionStateInfoMap.foreach { partitionState =>
        leaderCache.put(partitionState._1, partitionState._2)
        trace(("Broker %d cached leader info %s for partition %s in response to UpdateMetadata request " +
          "sent by controller %d epoch %d with correlation id %d").format(brokerId, partitionState._2, partitionState._1,
          updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch, updateMetadataRequest.correlationId))
      }
    }
    new UpdateMetadataResponse(updateMetadataRequest.correlationId)
  }
}
