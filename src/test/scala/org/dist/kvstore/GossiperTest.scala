package org.dist.kvstore

import org.scalatest.FunSuite

class GossiperTest extends FunSuite {

  test("should initialize seed list without local endpoint") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val gossiper = new Gossiper(1, InetAddressAndPort.create("127.0.0.1", 8000),
      DatabaseConfiguration(seeds))
    assert(gossiper.seeds.isEmpty)
  }

  test("should initialize seed list with all non local seeds") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000), InetAddressAndPort.create("127.0.0.1", 8002))
    val gossiper = new Gossiper(1, InetAddressAndPort.create("127.0.0.1", 8000),
      DatabaseConfiguration(seeds))

    assert(gossiper.seeds.get(0) ==  InetAddressAndPort.create("127.0.0.1", 8002))
  }

  test("should initialize endpoint state for local endpoint") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000), InetAddressAndPort.create("127.0.0.1", 8002))
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val generationNbr = 1
    val gossiper = new Gossiper(generationNbr, localEndpoint,
      DatabaseConfiguration(seeds))

    val localState: EndPointState = gossiper.endpointStatemap.get(localEndpoint)
    assert(localState.heartBeatState.generation == generationNbr)
    assert(localState.heartBeatState.version == VersionGenerator.currentVersion)
    assert(localState.applicationStates.isEmpty)
  }

  test("live endpoints and unreachables endpoints lists should be empty at initialization") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000), InetAddressAndPort.create("127.0.0.1", 8002))
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val generationNbr = 1
    val gossiper = new Gossiper(generationNbr, localEndpoint,
      DatabaseConfiguration(seeds))
    assert(gossiper.liveEndpoints.isEmpty)
    assert(gossiper.unreachableEndpoints.isEmpty)
  }
}
