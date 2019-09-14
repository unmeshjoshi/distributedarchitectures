package org.dist.kvstore

import java.util
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, ScheduledThreadPoolExecutor, TimeUnit}
import java.util.{Collections, Random}

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


class Gossiper(private[kvstore] val generationNbr: Int,
               private[kvstore] val localEndpoint: InetAddressAndPort,
               private[kvstore] val config: DatabaseConfiguration,
               private[kvstore] val executor: ScheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1),
               private[kvstore] val liveEndpoints:util.List[InetAddressAndPort] = new util.ArrayList[InetAddressAndPort],
               private[kvstore] val unreachableEndpoints:util.List[InetAddressAndPort] = new util.ArrayList[InetAddressAndPort]) {

  private[kvstore] val logger = LoggerFactory.getLogger(classOf[Gossiper])

  private[kvstore] val seeds = config.nonLocalSeeds(localEndpoint)
  private[kvstore] val endpointStatemap = new ConcurrentHashMap[InetAddressAndPort, EndPointState]

  initializeLocalEndpointState()


  private val taskLock = new ReentrantLock
  private val random: Random = new Random


  def initializeLocalEndpointState() = {
    var localState = endpointStatemap.get(localEndpoint)
    if (localState == null) {
      val hbState = HeartBeatState(generationNbr, 0)
      localState = EndPointState(hbState, Collections.emptyMap())
      endpointStatemap.put(localEndpoint, localState)
    }
  }

  private val intervalMillis = 1000

  def start() = {
    executor.scheduleAtFixedRate(new GossipTask, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS)

  }

  class GossipTask extends Runnable {

    @Override
    def run() {
      try {
//        //wait on messaging service to start listening
//        MessagingService.instance().waitUntilListening()
        taskLock.lock()
        updateLocalHeartbeatCounter
        val randomDigest = new GossipDigestBuilder().makeRandomGossipDigest()

      } finally {
        taskLock.unlock()
      }
    }

    private def updateLocalHeartbeatCounter = {
      /* Update the local heartbeat counter. */
      val state = endpointStatemap.get(localEndpoint)
      val newState = state.copy(state.heartBeatState.updateVersion())
      endpointStatemap.put(localEndpoint, newState)
    }
  }


  class GossipDigestBuilder {
    /**
     * No locking required since it is called from a method that already
     * has acquired a lock. The gossip digest is built based on randomization
     * rather than just looping through the collection of live endpoints.
     *
     */
    def makeRandomGossipDigest() = {
      val digests: util.List[GossipDigest] = new util.ArrayList[GossipDigest]()
      /* Add the local endpoint state */
      var epState: EndPointState = endpointStatemap.get(localEndpoint)
      var generation = epState.heartBeatState.generation
      var maxVersion = epState.getMaxEndPointStateVersion
      val localDigest = new GossipDigest(localEndpoint, generation, maxVersion)

      digests.add(localDigest)

      val endpoints = new util.ArrayList[InetAddressAndPort](liveEndpoints)
      Collections.shuffle(endpoints, random)

      for (liveEndPoint <- endpoints.asScala) {
        epState = endpointStatemap.get(liveEndPoint)
        if (epState != null) {
          generation = epState.heartBeatState.generation
          maxVersion = epState.getMaxEndPointStateVersion
          digests.add(new GossipDigest(liveEndPoint, generation, maxVersion))
        }
        else digests.add(new GossipDigest(liveEndPoint, 0, 0))
      }

      log(digests)

      digests
    }

  }

  private def log(gDigests: util.List[GossipDigest]) = {
    /* FOR DEBUG ONLY - remove later */ val sb = new StringBuilder
    for (gDigest <- gDigests.asScala) {
      sb.append(gDigest)
      sb.append(" ")
    }
    logger.trace("Gossip Digests are : " + sb.toString)
  }
}
