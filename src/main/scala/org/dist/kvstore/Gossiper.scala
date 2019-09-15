package org.dist.kvstore

import java.util
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, ScheduledThreadPoolExecutor, TimeUnit}
import java.util.{Collections, Random}

import org.dist.dbgossip.{Stage, Verb}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


class Gossiper(private[kvstore] val generationNbr: Int,
               private[kvstore] val localEndPoint: InetAddressAndPort,
               private[kvstore] val config: DatabaseConfiguration,
               private[kvstore] val executor: ScheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1),
               private[kvstore] val messagingService:MessagingService,
  private[kvstore] val liveEndpoints: util.List[InetAddressAndPort] = new util.ArrayList[InetAddressAndPort],
  private[kvstore] val unreachableEndpoints: util.List[InetAddressAndPort] = new util.ArrayList[InetAddressAndPort]) {

  private[kvstore] val logger = LoggerFactory.getLogger(classOf[Gossiper])

  private[kvstore] val seeds = config.nonLocalSeeds(localEndPoint)
  private[kvstore] val endpointStatemap = new ConcurrentHashMap[InetAddressAndPort, EndPointState]

  initializeLocalEndpointState()


  private val taskLock = new ReentrantLock
  private val random: Random = new Random
  private val intervalMillis = 1000

  def initializeLocalEndpointState() = {
    var localState = endpointStatemap.get(localEndPoint)
    if (localState == null) {
      val hbState = HeartBeatState(generationNbr, 0)
      localState = EndPointState(hbState, Collections.emptyMap())
      endpointStatemap.put(localEndPoint, localState)
    }
  }

  def start() = {
    executor.scheduleAtFixedRate(new GossipTask, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS)
  }

  private def log(gDigests: util.List[GossipDigest]) = {
    /* FOR DEBUG ONLY - remove later */ val sb = new StringBuilder
    for (gDigest <- gDigests.asScala) {
      sb.append(gDigest)
      sb.append(" ")
    }
    logger.trace("Gossip Digests are : " + sb.toString)
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
        val gossipDigestSynMessage = new GossipSynMessageBuilder().makeGossipDigestSynMessage(randomDigest)

        val sentToSeedNode = doGossipToLiveMember(gossipDigestSynMessage)
        /* Gossip to some unreachable member with some probability to check if he is back up */
        doGossipToUnreachableMember(gossipDigestSynMessage)

        if (!sentToSeedNode) { //If live members chosen to send gossip already had seed node, dont send message to seed
          doGossipToSeed(gossipDigestSynMessage)
        }
      } finally {
        taskLock.unlock()
      }
    }

    private def doGossipToSeed(message: Message): Unit = {
      val size = seeds.size
      if (size > 0) {
        if (size == 1 && seeds.contains(localEndPoint)) return
        if (liveEndpoints.size == 0) sendGossip(message, seeds)
        else {
          /* Gossip with the seed with some probability. */
          val probability = seeds.size / (liveEndpoints.size + unreachableEndpoints.size)
          val randDbl = random.nextDouble
          if (randDbl <= probability) sendGossip(message, seeds)
        }
      }
    }

    private def doGossipToUnreachableMember(message: Message): Unit = {
      val liveEndPoints = liveEndpoints.size
      val unreachableEndPoints = unreachableEndpoints.size
      if (unreachableEndPoints > 0) {
        /* based on some probability */ val prob = unreachableEndPoints / (liveEndPoints + 1)
        val randDbl = random.nextDouble
        if (randDbl < prob) sendGossip(message, unreachableEndpoints)
      }
    }

    //@return true if the chosen endpoint is also a seed.
    private def sendGossip(message: Message, epSet: util.List[InetAddressAndPort]) = {
      val size = epSet.size
      /* Generate a random number from 0 -> size */ val liveEndPoints = new util.ArrayList[InetAddressAndPort](epSet)
      val index = if (size == 1) 0
      else random.nextInt(size)
      val to = liveEndPoints.get(index)
      logger.trace("Sending a GossipDigestSynMessage to " + to + " ...")
      messagingService.sendUdpOneWay(message, to)
      seeds.contains(to)
    }

    private def doGossipToLiveMember(message: Message): Boolean = {
      val size = liveEndpoints.size
      if (size == 0) return false
      // return sendGossipToLiveNode(message);
      /* Use this for a cluster size >= 30 */ sendGossip(message, liveEndpoints)
    }

    private def updateLocalHeartbeatCounter = {
      /* Update the local heartbeat counter. */
      val state = endpointStatemap.get(localEndPoint)
      val newState = state.copy(state.heartBeatState.updateVersion())
      endpointStatemap.put(localEndPoint, newState)
    }
  }

  class GossipSynMessageBuilder {
    def makeGossipDigestSynMessage(gDigests: util.List[GossipDigest]) = {
      val gDigestMessage = new GossipDigestSyn(config.getClusterName(), gDigests)
      val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_SYN)
      Message(header, JsonSerDes.serialize(gDigestMessage))
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
      var epState: EndPointState = endpointStatemap.get(localEndPoint)
      var generation = epState.heartBeatState.generation
      var maxVersion = epState.getMaxEndPointStateVersion
      val localDigest = new GossipDigest(localEndPoint, generation, maxVersion)

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

}
