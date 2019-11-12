package org.dist.kvstore

import java.util.concurrent.ScheduledThreadPoolExecutor
import scala.jdk.CollectionConverters._


class GossiperTestBuilder(val seeds:Set[InetAddressAndPort] = Set(InetAddressAndPort.create("127.0.0.1", 8000)),
                          val localEndpoint:InetAddressAndPort = InetAddressAndPort.create("127.0.0.1", 8000),
                          val executor:ScheduledThreadPoolExecutor = new TestScheduledThreadPoolExecutor,
                          val messagingService:MessagingService = new TestMessagingService) {
  def build() = gossiper


  private val gossiper = new Gossiper(1, localEndpoint, DatabaseConfiguration(seeds), executor, messagingService)

  def withEndpointState(host: String, port: Int, hearBeatVersion: Int = 0, applicationStates: Map[ApplicationState, VersionedValue], generation: Int) = {
    val ep = EndPointState(HeartBeatState(generation, hearBeatVersion), applicationStates.asJava)
    gossiper.endpointStatemap.put(InetAddressAndPort.create(host, port), ep)
    this
  }

  def withLiveEndpoints(hosts:List[(String, Int)]) = {
    hosts.map(hostPort => gossiper.liveEndpoints.add(InetAddressAndPort.create(hostPort._1, hostPort._2)))
    this
  }

  def withUnReachableEndpoints(hosts:List[(String, Int)]) = {
    hosts.map(hostPort => gossiper.unreachableEndpoints.add(InetAddressAndPort.create(hostPort._1, hostPort._2)))
    this
  }
}

