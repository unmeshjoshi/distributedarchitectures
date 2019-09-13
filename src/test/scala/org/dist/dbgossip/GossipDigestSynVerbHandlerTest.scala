package org.dist.dbgossip

import java.math.BigInteger
import java.net.InetAddress
import java.util

import org.scalatest.FunSuite

class GossipDigestSynVerbHandlerTest extends FunSuite {
  test("should send GossipDigestAckMessage with delta gossip digest and endpoint state") {
    val gossip = gossipDigestSyn("127.0.0.1", 8080)
    gossiperState("127.0.0.1", 8081, newToken())

    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    Gossiper.instance.examineGossiper(gossip.gDigests, deltaGossipDigest, deltaEndPointStates)

    val expectedDeltaGossipDigests = new util.ArrayList[GossipDigest]();
    expectedDeltaGossipDigests.add(new GossipDigest(InetAddressAndPort.create("127.0.0.1", 8080), 1, 0))
    val expectedDeltaEndpointStates = new util.HashMap[InetAddressAndPort, EndPointState]()

    assert(InetAddressAndPort.create("127.0.0.1", 8080) == InetAddressAndPort.create("127.0.0.1", 8080))
    assert(deltaGossipDigest == expectedDeltaGossipDigests)
    assert(deltaEndPointStates == expectedDeltaEndpointStates)
  }

  private def gossiperState(host: String, port: Int, token: String) = {
    val applicationState: util.Map[ApplicationState, VersionedValue] = new util.HashMap[ApplicationState, VersionedValue]()
    applicationState.put(ApplicationState.TOKENS, new VersionedValue(token, 1))
    val state = new EndPointState(new HeartBeatState(1, 1), applicationState)
    Gossiper.instance.endpointStateMap.put(InetAddressAndPort.create(host, port), state)
  }

  private def gossipDigestSyn(host: "127.0.0.1", port: 8080) = {
    val digest = new GossipDigest(InetAddressAndPort.create(host, port), 1, 1)
    new GossipDigestSyn("cluster1", util.Arrays.asList(digest))
  }

  def newToken() = {
    val guid: String = GuidGenerator.guid
    var token: BigInteger = FBUtilities.hash(guid)
    if (token.signum == -1) token = token.multiply(BigInteger.valueOf(-1L))
    token.toString
  }
}
