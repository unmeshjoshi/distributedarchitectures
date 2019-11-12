package org.dist.kvstore

import org.scalatest.FunSuite
import scala.jdk.CollectionConverters._

class GossiperTaskTest extends FunSuite {

  test("Should update heartbeat at every run") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService

    val localEp = InetAddressAndPort.create("127.0.0.1", 8000)
    val gossiper = new Gossiper(1, localEp,
      DatabaseConfiguration(seeds), executor, messagingService)

    val task = new gossiper.GossipTask()
    task.run()
    task.run()

    val state = gossiper.endpointStatemap.get(localEp)
    assert(state.heartBeatState.heartBeat.get() == 2)
  }

  test("Should update heartbeat version when heartbeat is updated") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService

    val localEp = InetAddressAndPort.create("127.0.0.1", 8000)
    val gossiper = new Gossiper(1, localEp,
      DatabaseConfiguration(seeds), executor, messagingService)

    val task = new gossiper.GossipTask()
    task.run()
    task.run()

    val state = gossiper.endpointStatemap.get(localEp)
    assert(state.heartBeatState.version == 2)
  }
}
