package org.dist.rapid

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.patterns.replicatedlog.{Client, TcpListener}
import org.dist.queue.api.RequestOrResponse
import org.dist.rapid.messages.{AlertMessage, JoinMessage, JoinResponse, RapidMessages}

class MembershipService(listenAddress:InetAddressAndPort, view:MembershipView) {
  val tcpListener = new TcpListener(listenAddress, handle)
  def start(): Unit = {
      tcpListener.start()
  }

  def handle(request:RequestOrResponse):RequestOrResponse = {
    if (request.requestId == RapidMessages.preJoinMessage) {
      view.isSafeToJoin()
      val observer = view.getObservers()
      val response = JsonSerDes.serialize(List(observer))
      RequestOrResponse(RapidMessages.joinPhase1Response, response, request.correlationId)

    } else if(request.requestId == RapidMessages.joinMessage) {
      val joinMessage = JsonSerDes.deserialize(request.messageBodyJson, classOf[JoinMessage])
      val alertMessage = AlertMessage(AlertMessage.UP, joinMessage.address)
      view.endpoints.forEach(endpoint => {
        val r = RequestOrResponse(RapidMessages.joinMessage, JsonSerDes.serialize(alertMessage), request.correlationId)
        new Client().sendOneWay(r, endpoint)
      })
      RequestOrResponse(0, "", 0)
    } else if (request.requestId == RapidMessages.alertMessage) {
      val alertMessage = JsonSerDes.deserialize(request.messageBodyJson, classOf[AlertMessage])
      RequestOrResponse(0, "", 0)
    }
    else {
      RequestOrResponse(0, "", 0)
    }
  }
}
