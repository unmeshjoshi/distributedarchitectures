package org.dist.kvstore

import java.util
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, ScheduledThreadPoolExecutor, TimeUnit}
import java.util.{ArrayList, Collections, List, Random}

import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.control.Breaks
import scala.util.control.Breaks.breakable

trait IEndPointStateChangeSubscriber {
  /**
   * Use to inform interested parties about the change in the state
   * for specified endpoint
   *
   * @param endpoint endpoint for which the state change occured.
   * @param epState  state that actually changed for the above endpoint.
   */
  def onChange(endpoint: InetAddressAndPort, epState: EndPointState): Unit
}

class Gossiper(private[kvstore] val generationNbr: Int,
               private[kvstore] val localEndPoint: InetAddressAndPort,
               private[kvstore] val config: DatabaseConfiguration,
               private[kvstore] val executor: ScheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1),
               private[kvstore] val messagingService: MessagingService,
               private[kvstore] val liveEndpoints: util.List[InetAddressAndPort] = new util.ArrayList[InetAddressAndPort],
               private[kvstore] val unreachableEndpoints: util.List[InetAddressAndPort] = new util.ArrayList[InetAddressAndPort]) {

  private[kvstore] val versionGenerator = new VersionGenerator()
  private[kvstore] val logger = LoggerFactory.getLogger(classOf[Gossiper])
  private[kvstore] val seeds = config.nonLocalSeeds(localEndPoint)
  private[kvstore] val endpointStatemap = new ConcurrentHashMap[InetAddressAndPort, EndPointState]
  private val subscribers = new util.ArrayList[IEndPointStateChangeSubscriber]

  private val taskLock = new ReentrantLock
  private val random: Random = new Random

  initializeLocalEndpointState()
  private val intervalMillis = 1000

  def addApplicationState(state: ApplicationState, value: String) = this.synchronized {
    val localEndpointState = endpointStatemap.get(localEndPoint)
    val newState = localEndpointState.addApplicationState(state, VersionedValue(value, versionGenerator.incrementAndGetVersion))
    endpointStatemap.put(localEndPoint, newState)
  }

  def notifyFailureDetector(epStateMap: util.Map[InetAddressAndPort, EndPointState]) = {

  }

  def initializeLocalEndpointState() = {
    var localState = endpointStatemap.get(localEndPoint)
    if (localState == null) {
      val hbState = HeartBeatState(generationNbr, 0)
      localState = EndPointState(hbState, Collections.emptyMap())
      endpointStatemap.put(localEndPoint, localState)
    }
    messagingService.init(this)
  }

  def register (subscriber: IEndPointStateChangeSubscriber): Unit = {
    subscribers.add (subscriber)
  }

  def start() = {
    executor.scheduleAtFixedRate(new GossipTask, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS)
  }

  def applyStateLocally(epStateMap: util.Map[InetAddressAndPort, EndPointState]): Unit = this.synchronized {
    val eps = epStateMap.keySet.asScala
    for (ep <- eps) {
      breakable {
        if (ep == localEndPoint)
          Breaks.break()

        val localEpStatePtr = endpointStatemap.get(ep)
        val remoteState = epStateMap.get(ep)
        /*
                If state does not exist just add it. If it does then add it only if the version
                of the remote copy is greater than the local copy.
            */
        if (localEpStatePtr != null) {
          val localGeneration: Int = localEpStatePtr.heartBeatState.generation
          val remoteGeneration: Int = remoteState.heartBeatState.generation
          if (remoteGeneration > localGeneration)
            handleNewJoin(ep, remoteState)
          else if (remoteGeneration == localGeneration) {
            /* manage the membership state */
            val localMaxVersion = localEpStatePtr.getMaxEndPointStateVersion()
            val remoteMaxVersion = remoteState.getMaxEndPointStateVersion()
            if (remoteMaxVersion > localMaxVersion) {
              resusitate(ep, localEpStatePtr)
              val newHeartbeatState = applyHeartBeatStateLocally(ep, localEpStatePtr, remoteState)
              //TODO This has to be implemented
              /* apply ApplicationState */
              //              applyApplicationStateLocally(ep, localEpStatePtr, remoteState)
            }
          }
        } else {
          handleNewJoin(ep, remoteState)
        }
      }
    }
  }

  def resusitate(addr: InetAddressAndPort, localState: EndPointState) = {
    logger.debug("Attempting to resusitate " + addr)
    markLiveOrUnreachable(addr, localState, true)
    logger.debug("EndPoint " + addr + " is now UP")
  }

  def doNotifications(ep: InetAddressAndPort, epState: EndPointState): Unit = {
    logger.info(s"notifications for ${ep} ${epState}")
    for(subscriber <- subscribers.asScala) {
      subscriber.onChange(ep, epState)
    }
  }

  private[kvstore] def applyHeartBeatStateLocally(addr: InetAddressAndPort, localState: EndPointState, remoteState: EndPointState): Unit = {
    val localHbState = localState.heartBeatState
    val remoteHbState = remoteState.heartBeatState
    if (remoteHbState.generation > localHbState.generation) {
      resusitate(addr, localState)
      return localState.copy(remoteHbState)
    }
    if (localHbState.generation == remoteHbState.generation) {
      if (remoteHbState.version > localHbState.version) {
        val oldVersion = localHbState.version

        logger.debug("Updating heartbeat state version to " + localState.heartBeatState.version + " from " + oldVersion + " for " + addr + " ...")
        return localState.copy(remoteHbState)
      }
    }
    localState
  }

  private def handleNewJoin(ep: InetAddressAndPort, epState: EndPointState): Unit = {
    logger.info("Node " + ep + " has now joined.")
    /* Mark this endpoint as "live" */
    endpointStatemap.put(ep, epState)
    markLiveOrUnreachable(ep, epState, true)
    logger.info(s"Enpoint State Map for ${localEndPoint} is ${endpointStatemap}")
    /* Notify interested parties about endpoint state change */
    doNotifications(ep, epState)
  }

  private[kvstore] def markLiveOrUnreachable(addr: InetAddressAndPort, epState: EndPointState, value: Boolean): Unit = {
    if (value) {
      liveEndpoints.add(addr)
      unreachableEndpoints.remove(addr)
    }
    else {
      liveEndpoints.remove(addr)
      unreachableEndpoints.add(addr)
    }
  }

  private def log(gDigests: util.Set[GossipDigest]) = {
    /* FOR DEBUG ONLY - remove later */ val sb = new StringBuilder
    for (gDigest <- gDigests.asScala) {
      sb.append(gDigest)
      sb.append(" ")
    }
    logger.trace("Gossip Digests are : " + sb.toString)
  }

  /*
        This method is used to figure the state that the Gossiper has but Gossipee doesn't. The delta digests
        and the delta state are built up.
    */
  private[kvstore] def examineGossiper(digestList: util.List[GossipDigest], deltaGossipDigestList: util.List[GossipDigest], deltaEpStateMap: util.Map[InetAddressAndPort, EndPointState]): Unit = {
    for (gDigest <- digestList.asScala) {
      breakable {
        val remoteGeneration = gDigest.generation
        val maxRemoteVersion = gDigest.maxVersion
        /* Get state associated with the end point in digest */
        val endpointState = endpointStatemap.get(gDigest.endPoint)
        /*
      Here we need to fire a GossipDigestAckMessage. If we have some data associated with this endpoint locally
      then we follow the "if" path of the logic. If we have absolutely nothing for this endpoint we need to
      request all the data for this endpoint.
     */ if (endpointState != null) {
          val localGeneration = endpointState.heartBeatState.generation
          val maxLocalVersion = endpointState.getMaxEndPointStateVersion()
          if (remoteGeneration == localGeneration && maxRemoteVersion == maxLocalVersion) {
            Breaks.break() //todo: continue is not supported}
          }
          if (remoteGeneration > localGeneration)
          /* we request everything from the gossiper */
            requestAll(gDigest, deltaGossipDigestList, remoteGeneration)
          if (remoteGeneration < localGeneration)
          /* send all data with generation = localgeneration and version > 0 */
            sendAll(gDigest, deltaEpStateMap, 0)
          if (remoteGeneration == localGeneration) {
            /*
                                 If the max remote version is greater then we request the remote endpoint send us all the data
                                 for this endpoint with version greater than the max version number we have locally for this
                                 endpoint.
                                 If the max remote version is lesser, then we send all the data we have locally for this endpoint
                                 with version greater than the max remote version.
          */
            if (maxRemoteVersion > maxLocalVersion)
              deltaGossipDigestList.add(new GossipDigest(gDigest.endPoint, remoteGeneration, maxLocalVersion))
            if (maxRemoteVersion < maxLocalVersion)
            /* send all data with generation = localgeneration and version > maxRemoteVersion */
              sendAll(gDigest, deltaEpStateMap, maxRemoteVersion)
          }
        }
        else {
          /* We are here since we have no data for this endpoint locally so request everthing. */
          requestAll(gDigest, deltaGossipDigestList, remoteGeneration)
        }
      }
    }
  }

  /**
   * initial gossipdigest empty endpoint state
   * endpoint state having same generation same version
   * endpoint state having same generation lower version
   * endpoint state having same generation higher version
   * endpoint state having lower generation than remote
   * send only endpoint state higher than the remote version
   */


  /* Request all the state for the endpoint in the gDigest */
  private[kvstore] def requestAll(gDigest: GossipDigest, deltaGossipDigestList: util.List[GossipDigest], remoteGeneration: Int): Unit = {
    /* We are here since we have no data for this endpoint locally so request everthing. */
    deltaGossipDigestList.add(new GossipDigest(gDigest.endPoint, remoteGeneration, 0))
  }

  /* Send all the data with version greater than maxRemoteVersion */
  private[kvstore] def sendAll(gDigest: GossipDigest, deltaEpStateMap: util.Map[InetAddressAndPort, EndPointState], maxRemoteVersion: Int): Unit = {
    val localEpStatePtr = getStateForVersionBiggerThan(gDigest.endPoint, maxRemoteVersion)
    if (localEpStatePtr != null) deltaEpStateMap.put(gDigest.endPoint, localEpStatePtr)
  }

  import scala.util.control.Breaks._

  private[kvstore] def getStateForVersionBiggerThan(forEndpoint: InetAddressAndPort, version: Int) = {
    val epState = endpointStatemap.get(forEndpoint)
    var reqdEndPointState: EndPointState = null
    if (epState != null) {
      /*
                  * Here we try to include the Heart Beat state only if it is
                  * greater than the version passed in. It might happen that
                  * the heart beat version maybe lesser than the version passed
                  * in and some application state has a version that is greater
                  * than the version passed in. In this case we also send the old
                  * heart beat and throw it away on the receiver if it is redundant.
                  */ val localHbVersion = epState.heartBeatState.version
      if (localHbVersion > version) reqdEndPointState = EndPointState(epState.heartBeatState)
      val appStateMap = epState.applicationStates
      /* Accumulate all application states whose versions are greater than "version" variable */ val keys = appStateMap.keySet
      for (key <- keys.asScala) {
        val versionValue = appStateMap.get(key)
        if (versionValue.version > version) {
          if (reqdEndPointState == null) reqdEndPointState = EndPointState(epState.heartBeatState)
          reqdEndPointState = reqdEndPointState.addApplicationState(key, versionValue)
        }
      }
    }
    reqdEndPointState
  }

  class GossipTask extends Runnable {

    @Override
    def run() = {
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
      } catch {
        case ex:Exception => ex.printStackTrace()
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
      /* Generate a random number from 0 -> size */
      val liveEndPoints = new util.ArrayList[InetAddressAndPort](epSet)
      val index = if (size == 1) 0
      else random.nextInt(size)
      val to = liveEndPoints.get(index)
      logger.trace("Sending a GossipDigestSynMessage to " + to + " ..." + "from " + message.header.from)
      messagingService.sendTcpOneWay(message, to)
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
      val newState = state.copy(state.heartBeatState.updateVersion(versionGenerator.incrementAndGetVersion))
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

  class GossipSynAckMessageBuilder {

    def makeGossipDigestAckMessage(deltaGossipDigest: util.ArrayList[GossipDigest], deltaEndPointStates: util.Map[InetAddressAndPort, EndPointState]) = {
      val map = deltaEndPointStates.asScala.toMap
      val gossipDigestAck = GossipDigestAck.create(deltaGossipDigest.asScala.toList, map)
      val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_ACK)
      Message(header, JsonSerDes.serialize(gossipDigestAck))
    }
  }


  class GossipAck2MessageBuilder {

    def makeGossipDigestAck2Message(deltaEndPointStates: util.Map[InetAddressAndPort, EndPointState]) = {
      val gossipDigestAck2 = GossipDigestAck2.create(deltaEndPointStates.asScala.toMap)
      val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_ACK2)
      Message(header, JsonSerDes.serialize(gossipDigestAck2))
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
      //FIXME Figure out why duplicates getting added here
      val digests = new util.HashSet[GossipDigest]()
      /* Add the local endpoint state */
      var epState = endpointStatemap.get(localEndPoint)
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
          digests.add(GossipDigest(liveEndPoint, generation, maxVersion))
        }
        else digests.add(GossipDigest(liveEndPoint, 0, 0))
      }

      log(digests)

      digests.asScala.toList.asJava
    }
  }
}
