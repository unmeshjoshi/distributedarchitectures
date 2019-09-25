package org.dist.queue

import org.I0Itec.zkclient.ZkClient
import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{LeaderAndIsrRequest, LeaderAndIsrResponse, RequestKeys, RequestOrResponse, UpdateMetadataRequest, UpdateMetadataResponse}
import org.dist.queue.network.{BoundedByteBufferSend, RequestChannel}
import org.dist.queue.network.RequestChannel.Response


class KafkaApis(val requestChannel: RequestChannel,
                val replicaManager: ReplicaManager,
                val zkClient: ZkClient,
                brokerId: Int,
                val controller: Controller) extends Logging {
  def handle(req: RequestChannel.Request): Unit = {
    info(s"processing ${req.requestObj} ************************************************ ")
    val request: RequestOrResponse = req.requestObj
    request.requestId match {
      case RequestKeys.UpdateMetadataKey ⇒ {
        println(s"Handling UpdateMetadataRequest ${request.messageBodyJson}")
//        val message = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[UpdateMetadataRequest])
        val send = new BoundedByteBufferSend(new RequestOrResponse(0, JsonSerDes.serialize(UpdateMetadataResponse(request.correlationId)), request.correlationId))
        requestChannel.sendResponse(new Response(req.processor, req, send))
      }
      case RequestKeys.LeaderAndIsrKey ⇒ {
        println(s"Handling LeaderAndIsrRequest ${request.messageBodyJson}" )
        val message = JsonSerDes.deserialize(request.messageBodyJson.getBytes, classOf[LeaderAndIsrRequest])
        val send = new BoundedByteBufferSend(new RequestOrResponse(0, JsonSerDes.serialize(LeaderAndIsrResponse(message.controllerId, Map(), 0)), message.correlationId))
        requestChannel.sendResponse(new Response(req.processor, req, send))
      }
    }


  }


}
