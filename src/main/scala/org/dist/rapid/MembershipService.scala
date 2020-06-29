package org.dist.rapid

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.patterns.replicatedlog.{Client, TcpListener}
import org.dist.queue.api.RequestOrResponse
import org.dist.rapid.messages.{AlertMessage, JoinMessage, JoinResponse, Phase1aMessage, Phase1bMessage, Phase2aMessage, Phase2bMessage, RapidMessages}

import scala.concurrent.{Future, Promise}
import java.util
import java.util.function.Consumer

import org.dist.queue.common.Logging
import org.dist.util.SocketIO

import scala.jdk.CollectionConverters._

class DecideViewChangeFunction(membershipService:MembershipService) extends Consumer[util.List[InetAddressAndPort]] with Logging {
  override def accept(endPoints: util.List[InetAddressAndPort]): Unit = {
    updateView(endPoints)
    respondToJoiners(endPoints)
    info(s"Resetting paxos instance in ${membershipService.listenAddress}")
    membershipService.paxos = new Paxos(membershipService.listenAddress, membershipService, membershipService.view.endpoints.size(), new DecideViewChangeFunction(membershipService))
  }

  private def respondToJoiners(endPoints: util.List[InetAddressAndPort]) = {
    endPoints.asScala.foreach(address => {
      val joinerSocketIO = membershipService.joiners.get(address)
      info(s"Responding to joiner ${address} ${joinerSocketIO}")
      if (joinerSocketIO != null) {
        val joinResponse = JoinResponse(membershipService.view.endpoints.asScala.toList)
        joinerSocketIO.write(RequestOrResponse(RapidMessages.joinMessage, JsonSerDes.serialize(joinResponse), 0))
        membershipService.joiners.remove(address)
      }
    })
  }

  private def updateView(endPoints: util.List[InetAddressAndPort]) = {
    endPoints.asScala.foreach(address => {
      membershipService.view.addEndpoint(address)
    })
  }
}

class MembershipService(val listenAddress:InetAddressAndPort, val view:MembershipView) extends Logging {
  val socketServer = new SocketServer(listenAddress, handle)
  var paxos = new Paxos(listenAddress, this, view.endpoints.size(), new DecideViewChangeFunction(this))

  def start(): Unit = {
      socketServer.start()
  }
  val joiners = new util.HashMap[InetAddressAndPort, SocketIO[RequestOrResponse]]

  def handle(request:RequestOrResponse, socketIO:SocketIO[RequestOrResponse]):Unit = {
    if (request.requestId == RapidMessages.preJoinMessage) {
      view.isSafeToJoin()
      val observer = view.getObservers()
      val response = JsonSerDes.serialize(JoinResponse(List(observer)))
      socketIO.write(RequestOrResponse(RapidMessages.joinPhase1Response, response, request.correlationId))

    } else if(request.requestId == RapidMessages.joinMessage) {
      val joinMessage = JsonSerDes.deserialize(request.messageBodyJson, classOf[JoinMessage])
      info(s"Waiting for joiner ${joinMessage.address} ${socketIO}")
      joiners.put(joinMessage.address, socketIO)

      val alertMessage = AlertMessage(AlertMessage.UP, joinMessage.address)
      view.endpoints.forEach(endpoint => {
        val r = RequestOrResponse(RapidMessages.alertMessage, JsonSerDes.serialize(alertMessage), request.correlationId)
        new Client().sendOneWay(r, endpoint)
      })

    } else if (request.requestId == RapidMessages.alertMessage) {
      val alertMessage = JsonSerDes.deserialize(request.messageBodyJson, classOf[AlertMessage])
      paxos.vval = List(alertMessage.address).asJava
      paxos.startPhase1a(1)

    } else if (request.requestId == RapidMessages.phase1aMessage) {
      paxos.handlePhase1aMessage(JsonSerDes.deserialize(request.messageBodyJson, classOf[Phase1aMessage]))

    } else if (request.requestId == RapidMessages.phase1bMessage) {
      paxos.handlePhase1bMessage(JsonSerDes.deserialize(request.messageBodyJson, classOf[Phase1bMessage]))

    } else if (request.requestId == RapidMessages.phase2aMessage) {
      paxos.handlePhase2aMessage(JsonSerDes.deserialize(request.messageBodyJson, classOf[Phase2aMessage]))

    } else if (request.requestId == RapidMessages.phase2bMessage) {
      paxos.handlePhase2bMessage(JsonSerDes.deserialize(request.messageBodyJson, classOf[Phase2bMessage]))

    }
  }
}
