package org.dist.queue

import org.I0Itec.zkclient.ZkClient
import org.dist.kvstore.JsonSerDes
import org.dist.queue.api._


class KafkaApis(val replicaManager: ReplicaManager,
                val zkClient: ZkClient,
                brokerId: Int,
                val controller: Controller) extends Logging {

  def handle(req: RequestOrResponse): RequestOrResponse = {
    info(s"processing ${req} ************************************************ ")
    val request: RequestOrResponse = req
    request.requestId match {
      case RequestKeys.UpdateMetadataKey ⇒ {
        println(s"Handling UpdateMetadataRequest ${request.messageBodyJson}")
//        val message = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[UpdateMetadataRequest])
        RequestOrResponse(0, JsonSerDes.serialize(UpdateMetadataResponse(request.correlationId)), request.correlationId)

      }
      case RequestKeys.LeaderAndIsrKey ⇒ {
        println(s"Handling LeaderAndIsrRequest ${request.messageBodyJson}" )
        val leaderAndIsrRequest: LeaderAndIsrRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[LeaderAndIsrRequest])
        replicaManager.becomeLeaderOrFollower(leaderAndIsrRequest)
        RequestOrResponse(0, JsonSerDes.serialize(LeaderAndIsrResponse(leaderAndIsrRequest.controllerId, Map(), 0)), leaderAndIsrRequest.correlationId)
      }
    }


  }


}
