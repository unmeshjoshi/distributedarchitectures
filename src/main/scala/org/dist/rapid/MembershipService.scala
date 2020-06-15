package org.dist.rapid

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.patterns.replicatedlog.{Client, TcpListener}
import org.dist.queue.api.RequestOrResponse
import org.dist.rapid.messages.{AlertMessage, JoinMessage, JoinResponse, Phase1aMessage, Phase1bMessage, Phase2aMessage, Phase2bMessage, RapidMessages}

import scala.jdk.CollectionConverters._

class MembershipService(listenAddress:InetAddressAndPort, view:MembershipView) {
  val tcpListener = new TcpListener(listenAddress, handle)
  var paxos = new Paxos(listenAddress, view.endpoints, view.endpoints.size(), (endPoints) => {
    println("Called this one")
  })

  def start(): Unit = {
      tcpListener.start()
  }

  def handle(request:RequestOrResponse):RequestOrResponse = {
    if (request.requestId == RapidMessages.preJoinMessage) {
      view.isSafeToJoin()
      val observer = view.getObservers()
      val response = JsonSerDes.serialize(JoinResponse(List(observer)))
      RequestOrResponse(RapidMessages.joinPhase1Response, response, request.correlationId)

    } else if(request.requestId == RapidMessages.joinMessage) {
      val joinMessage = JsonSerDes.deserialize(request.messageBodyJson, classOf[JoinMessage])
      val alertMessage = AlertMessage(AlertMessage.UP, joinMessage.address)
      view.endpoints.forEach(endpoint => {
        val r = RequestOrResponse(RapidMessages.alertMessage, JsonSerDes.serialize(alertMessage), request.correlationId)
        new Client().sendOneWay(r, endpoint)
      })
      RequestOrResponse(0, "", 0)

    } else if (request.requestId == RapidMessages.alertMessage) {
      val alertMessage = JsonSerDes.deserialize(request.messageBodyJson, classOf[AlertMessage])
      paxos.vval = List(alertMessage.address).asJava
      paxos.startPhase1a(1)
      RequestOrResponse(0, "", 0)

    } else if (request.requestId == RapidMessages.phase1aMessage) {
      paxos.handlePhase1aMessage(JsonSerDes.deserialize(request.messageBodyJson, classOf[Phase1aMessage]))
      RequestOrResponse(0, "", 0)
    } else if (request.requestId == RapidMessages.phase1bMessage) {
      paxos.handlePhase1bMessage(JsonSerDes.deserialize(request.messageBodyJson, classOf[Phase1bMessage]))
      RequestOrResponse(0, "", 0)
    } else if (request.requestId == RapidMessages.phase2aMessage) {
      paxos.handlePhase2aMessage(JsonSerDes.deserialize(request.messageBodyJson, classOf[Phase2aMessage]))
      RequestOrResponse(0, "", 0)
    } else if (request.requestId == RapidMessages.phase2bMessage) {
      paxos.handlePhase2bMessage(JsonSerDes.deserialize(request.messageBodyJson, classOf[Phase2bMessage]))
      RequestOrResponse(0, "", 0)
    }
    else {
      RequestOrResponse(0, "", 0)
    }
  }
}
