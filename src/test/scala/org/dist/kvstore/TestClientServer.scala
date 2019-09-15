package org.dist.kvstore

import java.math.BigInteger
import java.util

import org.dist.dbgossip.{FBUtilities, GuidGenerator, Stage, Verb}
import org.dist.kvstore
import org.dist.util.Networks

class GossipDigestMessageBuilder {

  def gossipDigestSyn(host: String, port: Int) = {
    val digest = gossipDigest(host, port)
    GossipDigestSyn("cluster1", util.Arrays.asList(digest))
  }

  private def gossipDigest(host: String, port: Int) = {
    kvstore.GossipDigest(InetAddressAndPort.create(host, port), 1, 0)
  }

  def newToken() = {
    val guid: String = GuidGenerator.guid
    var token: BigInteger = FBUtilities.hash(guid)
    if (token.signum == -1) token = token.multiply(BigInteger.valueOf(-1L))
    token.toString
  }
}

object TestClientServer extends App {
  val messagingService = new MessagingService()
  private val seedEp = InetAddressAndPort(new Networks().ipv4Address, 8080)
  messagingService.listen(seedEp)
  private val node1Ep = InetAddressAndPort(new Networks().ipv4Address, 8888)
  messagingService.listen(node1Ep)


  private val syn: GossipDigestSyn = new GossipDigestMessageBuilder().gossipDigestSyn("10.10.10.20", 8888)

  val header = Header(node1Ep, Stage.GOSSIP, Verb.GOSSIP_DIGEST_SYN)

  private val message = Message(header, JsonSerDes.serialize(syn))
  messagingService.sendTcpOneWay(message, seedEp)

}
