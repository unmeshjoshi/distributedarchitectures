package org.dist.kvstore

import java.math.BigInteger
import java.util

import org.dist.dbgossip.{FBUtilities, GuidGenerator}
import org.dist.kvstore
import org.scalatest.FunSuite

class GossipMessageHandlingTests extends FunSuite {
  /**
   * initial gossipdigest empty endpoint state
   * endpoint state having same generation same version
   * endpoint state having same generation lower version
   * endpoint state having same generation higher version
   * endpoint state having lower generation than remote
   * send only endpoint state higher than the remote version
   */

  test("should request all information from the endpoint if the state does not exist locally") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val builder = new GossiperTestBuilder(seeds)
    val gossiper = builder.build()

    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    val digest1 = gossipDigest("10.10.12.20", 8000)
    val digest2 = gossipDigest("10.10.12.21", 8000)
    gossiper.examineGossiper(util.Arrays.asList(digest1, digest2), deltaGossipDigest, deltaEndPointStates)

    assert(deltaGossipDigest.size() == 2)
    assert(deltaGossipDigest.get(0) == digest1)
    assert(deltaGossipDigest.get(1) == digest2)
  }



  private def gossipDigestSyn(host: String, port: Int) = {
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
