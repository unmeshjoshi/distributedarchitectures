package org.dist.kvstore

import java.math.BigInteger
import java.util

import org.dist.kvstore
import org.scalatest.FunSuite
import scala.jdk.CollectionConverters._

class GossipMessageHandlingTests extends FunSuite {
  /**
   * initial gossipdigest empty endpoint state - Done
   *#TODO
   * endpoint state having same generation same version - Done
   * endpoint state having same generation lower version - Done
   * endpoint state having same generation higher version - Done
   * endpoint state having lower generation than remote - Done
   * send only endpoint state higher than the remote version - Done
   */

  test("should request all information from the endpoint if the state does not exist locally") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val builder = new GossiperTestBuilder(seeds)
    val gossiper = builder.build()

    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    val digest1 = gossipDigest("10.10.12.20", 8000, 1, 0)
    val digest2 = gossipDigest("10.10.12.21", 8000, 1, 0)
    gossiper.examineGossiper(util.Arrays.asList(digest1, digest2), deltaGossipDigest, deltaEndPointStates)

    assert(deltaGossipDigest.size() == 2)
    assert(deltaGossipDigest.get(0) == digest1)
    assert(deltaGossipDigest.get(1) == digest2)
  }


  test("should request all generation is lower than that of remote") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val builder: GossiperTestBuilder = new GossiperTestBuilder(seeds)
    val states = Map(ApplicationState.TOKENS → VersionedValue("token", 1))
    builder.withEndpointState("10.10.12.20", 8080, 1, states, 1)
    val gossiper = builder.build()

    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    val digest = gossipDigest("10.10.12.20", 8080, 2, 1)
    gossiper.examineGossiper(util.Arrays.asList(digest), deltaGossipDigest, deltaEndPointStates)

    assert(deltaGossipDigest.size() == 1)
    assert(deltaGossipDigest.get(0) == gossipDigest("10.10.12.20", 8080, 2, 0))
  }

  test("should request all versioned values more than the max version available locally if remote generation is same") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val builder: GossiperTestBuilder = new GossiperTestBuilder(seeds)
    val states = Map(ApplicationState.TOKENS → VersionedValue("token", 1))
    builder.withEndpointState("10.10.12.20", 8080, 1, states, 1)
    val gossiper = builder.build()

    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    val digest = gossipDigest("10.10.12.20", 8080, 1, 2)
    gossiper.examineGossiper(util.Arrays.asList(digest), deltaGossipDigest, deltaEndPointStates)

    assert(deltaGossipDigest.size() == 1)
    assert(deltaGossipDigest.get(0) == gossipDigest("10.10.12.20", 8080, 1, 1))
  }

  test("should send all information for the endpoints if remote generation is less than generation available in the self") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val builder: GossiperTestBuilder = new GossiperTestBuilder(seeds)
    val states = Map(ApplicationState.TOKENS → VersionedValue("token", 1))
    builder.withEndpointState("10.10.12.20", 8080, 1, states, 2)
    val gossiper = builder.build()

    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    val digest = gossipDigest("10.10.12.20", 8080, 1, 1)
    gossiper.examineGossiper(util.Arrays.asList(digest), deltaGossipDigest, deltaEndPointStates)

    assert(deltaEndPointStates.size() == 1)
    assert(deltaEndPointStates.get(InetAddressAndPort.create("10.10.12.20", 8080)).applicationStates.asScala.toMap == Map(ApplicationState.TOKENS → VersionedValue("token", 1)))
  }

  test("should send all information with version more than remote endpoint both have same generation") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val builder: GossiperTestBuilder = new GossiperTestBuilder(seeds)
    val states = Map(ApplicationState.TOKENS → VersionedValue("token", 2))
    builder.withEndpointState("10.10.12.20", 8080, 1, states, 1)
    val gossiper = builder.build()

    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    val digest = gossipDigest("10.10.12.20", 8080, 1, 1)
    gossiper.examineGossiper(util.Arrays.asList(digest), deltaGossipDigest, deltaEndPointStates)

    assert(deltaEndPointStates.size() == 1)
    assert(deltaEndPointStates.get(InetAddressAndPort.create("10.10.12.20", 8080)).applicationStates.asScala.toMap == Map(ApplicationState.TOKENS → VersionedValue("token", 2)))
  }

  test("should send nothing if max version and generation is same") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val builder: GossiperTestBuilder = new GossiperTestBuilder(seeds)
    val states = Map(ApplicationState.TOKENS → VersionedValue("token", 1))
    builder.withEndpointState("10.10.12.20", 8080, 1, states, 1)
    val gossiper = builder.build()

    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    val digest = gossipDigest("10.10.12.20", 8080, 1, 1)
    gossiper.examineGossiper(util.Arrays.asList(digest), deltaGossipDigest, deltaEndPointStates)

    assert(deltaEndPointStates.size() == 0)
  }


  private def gossipDigestSyn(host: String, port: Int) = {
    val digest = gossipDigest(host, port)
    GossipDigestSyn("cluster1", util.Arrays.asList(digest))
  }

  private def gossipDigest(host: String, port: Int, generation: Int = 1, maxVersion: Int = 0) = {
    kvstore.GossipDigest(InetAddressAndPort.create(host, port), generation, maxVersion)
  }

  def newToken() = {
    val guid: String = GuidGenerator.guid
    var token: BigInteger = FBUtilities.hash(guid)
    if (token.signum == -1) token = token.multiply(BigInteger.valueOf(-1L))
    token.toString
  }
}
