package org.dist.rapid

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.patterns.replicatedlog.TcpListener
import org.dist.queue.api.RequestOrResponse
import org.dist.rapid.messages.{AlertMessage, JoinMessage, JoinResponse, Phase1aMessage, Phase1bMessage, Phase2aMessage, Phase2bMessage, RapidMessages}

import scala.concurrent.{Future, Promise}
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue, ScheduledExecutorService, ScheduledFuture, ThreadLocalRandom, ThreadPoolExecutor, TimeUnit}
import java.util.function.Consumer

import org.dist.queue.common.Logging
import org.dist.util.SocketIO

import scala.jdk.CollectionConverters._

class DecideViewChangeFunction(membershipService:MembershipService) extends Consumer[util.List[InetAddressAndPort]] with Logging {
  override def accept(endPoints: util.List[InetAddressAndPort]): Unit = {
    updateView(endPoints)
    respondToJoiners(endPoints)
    info(s"Resetting paxos instance in ${membershipService.listenAddress}")
    membershipService.announcedProposal.set(false)
    membershipService.paxos = new Paxos(membershipService.listenAddress, membershipService.view.endpoints.size(), new DecideViewChangeFunction(membershipService), new DefaultPaxosMessagingClient(membershipService.view))
    membershipService.cancelPaxosIfScheduled()
  }

  private def respondToJoiners(endPoints: util.List[InetAddressAndPort]) = {
    endPoints.asScala.foreach(address => {
      val joinerSocketIO = membershipService.joiners.get(address)
      info(s"Responding to joiner ${address} ${joinerSocketIO}")
      if (joinerSocketIO != null) {
        val joinResponse = JoinResponse(membershipService.view.endpoints.asScala.toList)
        joinerSocketIO.write(RequestOrResponse(RapidMessages.joinMessage, JsonSerDes.serialize(joinResponse), 0))
        membershipService.joiners.remove(address)
      }
    })
  }

  private def updateView(endPoints: util.List[InetAddressAndPort]) = {
    endPoints.asScala.foreach(address => {
      membershipService.view.addEndpoint(address)
    })
  }
}

class MembershipService(val listenAddress:InetAddressAndPort, val view:MembershipView) extends Logging {
  val socketServer = new SocketServer(listenAddress, handle)
  var paxos = new Paxos(listenAddress, view.endpoints.size(), new DecideViewChangeFunction(this), new DefaultPaxosMessagingClient(view))
  private val scheduledExecutorService: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  val decided = new AtomicBoolean(false)
  var announcedProposal = new AtomicBoolean(false)

  private var scheduledClassicRoundTask:ScheduledFuture[_] = null

  def cancelPaxosIfScheduled() = {
    if (scheduledClassicRoundTask != null) {
      scheduledClassicRoundTask.cancel(true)
    }
  }

  def start(): Unit = {
      socketServer.start()
  }

  val joiners = new util.HashMap[InetAddressAndPort, SocketIO[RequestOrResponse]]

  def handle(request:RequestOrResponse, socketIO:SocketIO[RequestOrResponse]):Unit = {
    if (request.requestId == RapidMessages.preJoinMessage) {
      view.isSafeToJoin()
      val observer = view.getObservers()
      val response = JsonSerDes.serialize(JoinResponse(List(observer)))
      socketIO.write(RequestOrResponse(RapidMessages.joinPhase1Response, response, request.correlationId))

    } else if(request.requestId == RapidMessages.joinMessage) {
      val joinMessage = JsonSerDes.deserialize(request.messageBodyJson, classOf[JoinMessage])
      info(s"Waiting for joiner ${joinMessage.address} ${socketIO}")
      joiners.put(joinMessage.address, socketIO)

      val alertMessage = AlertMessage(AlertMessage.UP, joinMessage.address)
      view.endpoints.forEach(endpoint => {
        val r = RequestOrResponse(RapidMessages.alertMessage, JsonSerDes.serialize(alertMessage), request.correlationId)
        new SocketClient().sendOneWay(r, endpoint)
      })

    } else if (request.requestId == RapidMessages.alertMessage) {
      val alertMessage = JsonSerDes.deserialize(request.messageBodyJson, classOf[AlertMessage])
      paxos.registerFastRoundVote(List(alertMessage.address).asJava)

      val ms: Long = getRandomDelayMs
      if (announcedProposal.get()) {
        return
      }
      announcedProposal.set(true)
      val runnable:Runnable = () => {
        startClassicPaxosRound()
      }
      scheduledClassicRoundTask = scheduledExecutorService.schedule(runnable, ms, TimeUnit.MILLISECONDS)


    } else if (request.requestId == RapidMessages.phase1aMessage) {
      paxos.handlePhase1aMessage(JsonSerDes.deserialize(request.messageBodyJson, classOf[Phase1aMessage]))

    } else if (request.requestId == RapidMessages.phase1bMessage) {
      paxos.handlePhase1bMessage(JsonSerDes.deserialize(request.messageBodyJson, classOf[Phase1bMessage]))

    } else if (request.requestId == RapidMessages.phase2aMessage) {
      paxos.handlePhase2aMessage(JsonSerDes.deserialize(request.messageBodyJson, classOf[Phase2aMessage]))

    } else if (request.requestId == RapidMessages.phase2bMessage) {
      paxos.handlePhase2bMessage(JsonSerDes.deserialize(request.messageBodyJson, classOf[Phase2bMessage]))

    }
  }

  private def getRandomDelayMs = {
    val rate = 1 / view.endpoints.size().toDouble
    val jitter = (-1000 * Math.log(1 - ThreadLocalRandom.current.nextDouble) / rate).toLong
    val ms = 1000 + jitter
    ms
  }

  def startClassicPaxosRound() = {
    paxos.startPhase1a(2)
  }
}
