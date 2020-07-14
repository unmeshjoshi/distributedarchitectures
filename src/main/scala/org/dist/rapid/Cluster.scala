package org.dist.rapid

import java.util

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api.RequestOrResponse
import org.dist.queue.common.Logging
import org.dist.rapid.messages.{JoinMessage, JoinResponse, PreJoinMessage, RapidMessages}

import scala.jdk.CollectionConverters._

class Cluster(listenAddress: InetAddressAndPort) extends Logging {
  var membershipService:MembershipService = _
  def start(): Unit = {
    val members = new util.ArrayList[InetAddressAndPort]()
    members.add(listenAddress)

    membershipService = new MembershipService(listenAddress, MembershipView(members))
    membershipService.start()
  }

  def join(seed: InetAddressAndPort) = {
    start() //start self
    val responses = joinAttempt(seed)
    val firstResponse = responses.get(0)
    val joinResponse = JsonSerDes.deserialize(firstResponse.messageBodyJson, classOf[JoinResponse])
    joinResponse.endPoints.foreach(address => membershipService.view.addEndpoint(address))
    info(s"View at ${listenAddress} is now ${membershipService.view.endpoints}")
  }

  private def joinAttempt(seed: InetAddressAndPort) = {
    val joinPhase1Response = joinPhase1(seed)
    val joinPhase2Response = joinPhase2(joinPhase1Response)
    joinPhase2Response
  }

  private def joinPhase2(joinPhase1Response: JoinResponse) = {
    val responses = new util.ArrayList[RequestOrResponse]()
    val observers = joinPhase1Response.endPoints
    observers.foreach(observer => {
      val message = JoinMessage(listenAddress)
      val response1 = new SocketClient().sendReceive(RequestOrResponse(RapidMessages.joinMessage, JsonSerDes.serialize(message), 1), observer)
      responses.add(response1)
    })
    responses
  }

  private def joinPhase1(seed: InetAddressAndPort) = {
    val preJoinMessage = PreJoinMessage(1, "node1")
    val client = new SocketClient()
    val response: RequestOrResponse = client.sendReceive(RequestOrResponse(RapidMessages.preJoinMessage, JsonSerDes.serialize(preJoinMessage), 0), seed)
    info(s"Received join phase1 response from ${seed} ${response.messageBodyJson}")
    val joinPhase1Response = JsonSerDes.deserialize(response.messageBodyJson, classOf[JoinResponse])
    joinPhase1Response
  }
}
