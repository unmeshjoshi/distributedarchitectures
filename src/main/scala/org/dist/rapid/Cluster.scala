package org.dist.rapid

import java.util

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.patterns.replicatedlog.Client
import org.dist.queue.api.RequestOrResponse
import org.dist.rapid.messages.{JoinMessage, JoinResponse, PreJoinMessage, RapidMessages}

import scala.jdk.CollectionConverters._

class Cluster(listenAddress: InetAddressAndPort) {
  def start(): Unit = {
    val membershipService = new MembershipService(listenAddress, MembershipView(List(listenAddress).asJava))
    membershipService.start()
  }

  def join(seed: InetAddressAndPort) = {
    start() //start self
    joinAttempt(seed)
  }

  private def joinAttempt(seed: InetAddressAndPort) = {
    val preJoinMessage = PreJoinMessage(1, "node1")
    val client = new Client()
    val response: RequestOrResponse = client.sendReceive(RequestOrResponse(RapidMessages.preJoinMessage, JsonSerDes.serialize(preJoinMessage), 0), seed)
    val joinResponse = JsonSerDes.deserialize(response.messageBodyJson, classOf[JoinResponse])
    val responses = new util.ArrayList[RequestOrResponse]()
    joinResponse.observers.foreach(observer => {
      val message = JoinMessage(listenAddress)
      val response1 = client.sendReceive(RequestOrResponse(RapidMessages.joinMessage, JsonSerDes.serialize(message), 1), observer)
      responses.add(response1)
    })
    responses
  }
}
