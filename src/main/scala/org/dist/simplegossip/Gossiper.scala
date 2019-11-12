package org.dist.simplegossip

import java.math.BigInteger
import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledThreadPoolExecutor, TimeUnit}
import java.util.{Collections, Random}

import org.dist.kvstore.{GossipDigest, GossipDigestSyn, Header, InetAddressAndPort, JsonSerDes, Message, Stage, Verb}
import org.dist.queue.common.Logging

import scala.jdk.CollectionConverters._

class Gossiper(val seed:InetAddressAndPort,
               val localEndPoint:InetAddressAndPort,
               val token:BigInteger,
               val executor: ScheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1)) extends Logging {

  def makeGossipDigestAck2Message(deltaEpStateMap: util.HashMap[InetAddressAndPort, BigInteger]) = {
    val gossipDigestAck2 = GossipDigestAck2(deltaEpStateMap.asScala.toMap)
    val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_ACK2)
    Message(header, JsonSerDes.serialize(gossipDigestAck2))
  }

  def getStateFor(addr: InetAddressAndPort) = {
    endpointStatemap.get(addr)
  }

  def handleNewJoin(ep: InetAddressAndPort, remoteToken: BigInteger) = {
    this.liveEndpoints.add(ep)
    this.endpointStatemap.put(ep, remoteToken)
    info(s"${ep} joined ${localEndPoint} ")
  }

  def applyStateLocally(epStateMap: Map[InetAddressAndPort, BigInteger]) = {
    val endPoints = epStateMap.keySet
    for(ep ← endPoints) {
      val remoteToken = epStateMap(ep)
      val token = this.endpointStatemap.get(ep)
      if (token == null) {
        handleNewJoin(ep, remoteToken)
      }
    }
  }


  def makeGossipDigestAckMessage(deltaGossipDigest: util.ArrayList[GossipDigest], deltaEndPointStates: util.HashMap[InetAddressAndPort, BigInteger]) = {
   val digestAck = GossipDigestAck(deltaGossipDigest.asScala.toList,deltaEndPointStates.asScala.toMap)
    val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_ACK)
    Message(header, JsonSerDes.serialize(digestAck))
  }

  def examineGossiper(digestList: util.List[GossipDigest], deltaGossipDigest: util.ArrayList[GossipDigest], deltaEndPointStates: util.HashMap[InetAddressAndPort, BigInteger]) = {
    for (gDigest <- digestList.asScala) {
      val endpointState = endpointStatemap.get(gDigest.endPoint)
      if (endpointState != null) {
        sendAll(gDigest, deltaEndPointStates)
      } else {
        requestAll(gDigest, deltaGossipDigest)
      }
    }
  }

  /* Request all the state for the endpoint in the gDigest */
  private def requestAll(gDigest: GossipDigest, deltaGossipDigestList: util.List[GossipDigest]): Unit = {
    /* We are here since we have no data for this endpoint locally so request everthing. */
    deltaGossipDigestList.add(new GossipDigest(gDigest.endPoint, 1, 0))
  }

  /* Send all the data with version greater than maxRemoteVersion */
  private def sendAll(gDigest: GossipDigest, deltaEpStateMap: util.Map[InetAddressAndPort, BigInteger]): Unit = {
    val localEpStatePtr = getStateFor(gDigest.endPoint)
    if (localEpStatePtr != null) deltaEpStateMap.put(gDigest.endPoint, localEpStatePtr)
  }


  val messagingService = new MessagingService(this)
  private val random: Random = new Random
  val liveEndpoints: util.List[InetAddressAndPort] = new util.ArrayList[InetAddressAndPort]
  val unreachableEndpoints: util.List[InetAddressAndPort] = new util.ArrayList[InetAddressAndPort]

  private val intervalMillis = 1000
  //simple state
  val endpointStatemap = new ConcurrentHashMap[InetAddressAndPort, BigInteger]
  //keep local state
  var localState = endpointStatemap.put(localEndPoint, token)

  def start() = {
    executor.scheduleAtFixedRate(new GossipTask, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS)
  }


  class GossipTask extends Runnable {

    @Override
    def run() = {
      val randomDigest = makeRandomGossipDigest()

      val gossipDigestSynMessage = makeGossipDigestSynMessage(randomDigest)

      val sentToSeedNode = doGossipToLiveMember(gossipDigestSynMessage)
      if (!sentToSeedNode) {
        doGossipToSeed(gossipDigestSynMessage)
      }
    }

    def makeGossipDigestSynMessage(gDigests: util.List[GossipDigest]) = {
      val gDigestMessage = new GossipDigestSyn("TestCluster", gDigests)
      val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_SYN)
      Message(header, JsonSerDes.serialize(gDigestMessage))
    }


    private def doGossipToSeed(message: Message): Unit = {
      val seeds = new util.ArrayList[InetAddressAndPort]()
      seeds.add(seed)

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

    private def doGossipToLiveMember(message: Message): Boolean = {
      val size = liveEndpoints.size
      if (size == 0) return false
      // return sendGossipToLiveNode(message);
      /* Use this for a cluster size >= 30 */ sendGossip(message, liveEndpoints)
    }

    //@return true if the chosen endpoint is also a seed.
    private def sendGossip(message: Message, epSet: util.List[InetAddressAndPort]) = {
      val size = epSet.size
      /* Generate a random number from 0 -> size */
      val liveEndPoints = new util.ArrayList[InetAddressAndPort](epSet)
      val index = if (size == 1) 0
      else random.nextInt(size)
      val to = liveEndPoints.get(index)

      info(s"Sending gossip message from ${localEndPoint} to ${to}")

      messagingService.sendTcpOneWay(message, to)
      seed == to
    }


    def makeRandomGossipDigest() = {
      //FIXME Figure out why duplicates getting added here
      val digests = new util.HashSet[GossipDigest]()
      /* Add the local endpoint state */
      var epState = endpointStatemap.get(localEndPoint)
      val localDigest = GossipDigest(localEndPoint, 1, 1)

      digests.add(localDigest)

      val endpoints = new util.ArrayList[InetAddressAndPort](liveEndpoints)
      Collections.shuffle(endpoints, random)

      for (liveEndPoint <- endpoints.asScala) {
        epState = endpointStatemap.get(liveEndPoint)
        if (epState != null) {
          digests.add(GossipDigest(liveEndPoint, 1, 1))
        }
        else digests.add(GossipDigest(liveEndPoint, 0, 0))
      }
      digests.asScala.toList.asJava
    }
  }
}
