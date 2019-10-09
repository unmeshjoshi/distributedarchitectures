package org.dist.kvstore

import org.scalatest.FunSuite

import scala.jdk.CollectionConverters._

class GossiperTest extends FunSuite {

  test("should initialize seed list without local endpoint") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService

    val gossiper = new Gossiper(1, InetAddressAndPort.create("127.0.0.1", 8000),
      DatabaseConfiguration(seeds), executor, messagingService)
    assert(gossiper.seeds.isEmpty)
  }

  test("should initialize seed list with all non local seeds") {
    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService

    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000), InetAddressAndPort.create("127.0.0.1", 8002))
    val gossiper = new Gossiper(1, InetAddressAndPort.create("127.0.0.1", 8000),
      DatabaseConfiguration(seeds), executor, messagingService)

    assert(gossiper.seeds.get(0) ==  InetAddressAndPort.create("127.0.0.1", 8002))
  }

  test("should initialize endpoint state for local endpoint") {
    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService

    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000), InetAddressAndPort.create("127.0.0.1", 8002))
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val generationNbr = 1
    val gossiper = new Gossiper(generationNbr, localEndpoint,
      DatabaseConfiguration(seeds), executor, messagingService)

    val localState: EndPointState = gossiper.endpointStatemap.get(localEndpoint)
    assert(localState.heartBeatState.generation == generationNbr)
    assert(localState.heartBeatState.version == gossiper.versionGenerator.currentVersion)
    assert(localState.applicationStates.isEmpty)
  }

  test("live endpoints and unreachables endpoints lists should be empty at initialization") {
    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService

    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000), InetAddressAndPort.create("127.0.0.1", 8002))
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val generationNbr = 1
    val gossiper = new Gossiper(generationNbr, localEndpoint,
      DatabaseConfiguration(seeds), executor, messagingService)
    assert(gossiper.liveEndpoints.isEmpty)
    assert(gossiper.unreachableEndpoints.isEmpty)
  }

  test("should make gossip digest builder from local and live endpoints") {
    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService

    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val gossiper = new Gossiper(1, localEndpoint,
      DatabaseConfiguration(seeds), executor, messagingService)

    gossiper.liveEndpoints.add(InetAddressAndPort.create("127.0.0.1", 8001))
    gossiper.liveEndpoints.add(InetAddressAndPort.create("127.0.0.1", 8002))


    val digests = new gossiper.GossipDigestBuilder().makeRandomGossipDigest()
    assert(digests.size() == 3)
    assert(digests.asScala.map(digest => digest.endPoint).contains(InetAddressAndPort.create("127.0.0.1", 8000)))
    assert(digests.asScala.map(digest => digest.endPoint).contains(InetAddressAndPort.create("127.0.0.1", 8002)))
    assert(digests.asScala.map(digest => digest.endPoint).contains(InetAddressAndPort.create("127.0.0.1", 8001)))
  }


  test("should contain maximum version of the local and live endpoints in digest") {
    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService

    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val gossiper = new Gossiper(1, localEndpoint,
      DatabaseConfiguration(seeds), executor, messagingService)

    val node1 = InetAddressAndPort.create("127.0.0.1", 8001)
    gossiper.liveEndpoints.add(node1)
    //Adding application state should increment version to 1
    val ep = EndPointState(HeartBeatState(1, gossiper.versionGenerator.incrementAndGetVersion), Map(ApplicationState.TOKENS → VersionedValue("1001", gossiper.versionGenerator.incrementAndGetVersion)).asJava)
    gossiper.endpointStatemap.put(node1, ep)

    val node2 = InetAddressAndPort.create("127.0.0.1", 8002)
    gossiper.liveEndpoints.add(node2)


    val digests = new gossiper.GossipDigestBuilder().makeRandomGossipDigest()
    val node1Digest = digests.asScala.filter(digest => digest.endPoint == node1)
    assert(node1Digest(0).maxVersion == 2)

    val localDigest = digests.asScala.filter(digest => digest.endPoint == localEndpoint)
    assert(localDigest(0).maxVersion == 0)
  }

  test("should construct GossipSynMessage with Gossip digests") {
    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService

    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val gossiper = new Gossiper(1, localEndpoint,
      DatabaseConfiguration(seeds), executor, messagingService)

    val node1 = InetAddressAndPort.create("127.0.0.1", 8001)
    gossiper.liveEndpoints.add(node1)
    //Adding application state should increment version to 1
    val ep = EndPointState(HeartBeatState(1, gossiper.versionGenerator.incrementAndGetVersion), Map(ApplicationState.TOKENS → VersionedValue("1001", gossiper.versionGenerator.incrementAndGetVersion)).asJava)
    gossiper.endpointStatemap.put(node1, ep)

    val node2 = InetAddressAndPort.create("127.0.0.1", 8002)
    gossiper.liveEndpoints.add(node2)

    val digests = new gossiper.GossipDigestBuilder().makeRandomGossipDigest()
    val message = new gossiper.GossipSynMessageBuilder().makeGossipDigestSynMessage(digests)
    assert(message.header.from == localEndpoint)
    assert(message.header.messageType == Stage.GOSSIP)
    assert(message.header.verb == Verb.GOSSIP_DIGEST_SYN)
  }
}
