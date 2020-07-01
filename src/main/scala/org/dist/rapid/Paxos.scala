package org.dist.rapid

import java.util.function.Consumer
import java.util
import java.util.{ArrayList, Collections, HashMap, List, Map}

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.patterns.replicatedlog.SocketClient
import org.dist.queue.api.RequestOrResponse
import org.dist.queue.common.Logging
import org.dist.rapid.messages.{Phase1aMessage, Phase1bMessage, Phase2aMessage, Phase2bMessage, Rank, RapidMessages}

import scala.jdk.CollectionConverters._
import scala.util.control.Breaks

class Paxos(address: InetAddressAndPort,
            membershipService: MembershipService,
            N: Int,
            consumer: Consumer[List[InetAddressAndPort]],
            var vval: util.List[InetAddressAndPort] = Collections.emptyList[InetAddressAndPort],
            var cval: util.List[InetAddressAndPort] = Collections.emptyList[InetAddressAndPort],
            var crnd: Rank = Rank(0, 0),
            var rnd: Rank = Rank(0, 0),
            var vrnd: Rank = Rank(0, 0)) extends Logging
{
  def registerFastRoundVote(joiningServerAddresses: util.List[InetAddressAndPort]) = {
    this.vval = joiningServerAddresses
    rnd = Rank(1, 1)
    vrnd = rnd //register initially for fast round.. we have skipped fast round in this implementation.
  }

  val configurationId = 0

  val socketClient = new SocketClient()
  private val phase1bMessages = new util.ArrayList[Phase1bMessage]
  private val acceptResponses = new HashMap[Rank, Map[InetAddressAndPort, Phase2bMessage]]


  def peers = membershipService.view.endpoints.asScala.toList.asJava

  def startPhase1a(round: Int): Unit = {
    if (crnd.round > round) //dont participiate in round higher than coordinator round.
      return

    info(s"Starting paxos phase1 at ${address}")

    crnd = Rank(round, address.hashCode())

    info(s"Sending paxos phase1 messages to ${peers}")
    val phase1aMessage = Phase1aMessage(0, address, crnd)
    broadcast(phase1aMessage)
  }

  def handlePhase1aMessage(phase1aMessage:Phase1aMessage): Unit = {
    if (configurationId != phase1aMessage.configurationId) {
      return
    }
    if (compareRanks(rnd, phase1aMessage.rank) < 0)
    rnd = phase1aMessage.rank
    else {
      trace(s"Rejecting prepareMessage from lower rank: (${rnd}) (${phase1aMessage.rank})")
      return
    }

    val phase1b = Phase1bMessage(0, rnd, address, vrnd, vval)
    socketClient.sendOneWay(RequestOrResponse(RapidMessages.phase1bMessage, JsonSerDes.serialize(phase1b), 0), phase1aMessage.address)
  }

  var decided = false

  def handlePhase1bMessage(phase1bMessage:Phase1bMessage):Unit = {
    if (phase1bMessage.configurationId != configurationId) return

    info(s"Got phase1b messages from ${phase1bMessage.sender}")
    // Only handle responses from crnd == i
    if (compareRanks(crnd, phase1bMessage.rnd) != 0) return

    phase1bMessages.add(phase1bMessage)
    if (phase1bMessages.size > (N / 2)) {
      val chosenProposal = selectProposalUsingCoordinatorRule(phase1bMessages)
      if (crnd == phase1bMessage.rnd && cval.isEmpty && !chosenProposal.isEmpty) {
        cval = chosenProposal
        info(s"Chosen proposal ${cval}")
        val message = Phase2aMessage(configurationId, address, crnd, chosenProposal)
        broadcast(message)
      }
    }
  }

  private def broadcast(message: Phase2aMessage) = {
    peers.asScala.foreach(addr => {
      val response = RequestOrResponse(RapidMessages.phase2aMessage, JsonSerDes.serialize(message), 0)
      socketClient.sendOneWay(response, addr)
    })
  }

  def handlePhase2aMessage(phase2aMessage:Phase2aMessage): Unit = {
    if (phase2aMessage.configurationId != configurationId) return

    trace(s"At acceptor received phase2aMessage: ${phase2aMessage}")
    if (compareRanks(rnd, phase2aMessage.crnd) <= 0 && !(vrnd == phase2aMessage.crnd)) {
      rnd = phase2aMessage.crnd
      vrnd = phase2aMessage.crnd
      vval = phase2aMessage.chosenProposal
      trace(s"${address} accepted value in vrnd: ${vrnd}, vval: ${vval}")
      val message = Phase2bMessage(configurationId, phase2aMessage.crnd, address, vval)

      trace(s"sending phase2b messages from ${address} to ${peers}")

      val phase2bMessage = RequestOrResponse(RapidMessages.phase2bMessage, JsonSerDes.serialize(message), 0)
      broadcast(phase2bMessage)
    }
  }

  private def broadcast(phase2bMessage: RequestOrResponse) = {
    peers.asScala.foreach(addr => {
      socketClient.sendOneWay(phase2bMessage, addr)
    })
  }

  def handlePhase2bMessage(phase2bMessage: Phase2bMessage):Unit = {
    if (phase2bMessage.configurationId != configurationId) {
      return
    }

    trace(s"Received phase2bMessage from ${phase2bMessage.sender}")
    val phase2bMessagesInRnd = acceptResponses.computeIfAbsent(phase2bMessage.rnd, (k: Rank) => new util.HashMap[InetAddressAndPort, Phase2bMessage])
    phase2bMessagesInRnd.put(phase2bMessage.sender, phase2bMessage)
    if (phase2bMessagesInRnd.size > (N / 2) && !decided) {
      val decision = phase2bMessage.vval
      debug(s"${address} decided on: ${decision} for rnd ${rnd} ${phase2bMessagesInRnd}")
      consumer.accept(decision)
      decided = true
    }
  }


  def selectProposalUsingCoordinatorRule(phase1bMessages: util.List[Phase1bMessage]) = {
    val maxVrndSoFar = phase1bMessages
      .asScala.map(phase1bMessage => phase1bMessage.vrnd)
      .max(compareRanks)
    // Let k be the largest value of vr(a) for all a in Q.
    // V (collectedVvals) be the set of all vv(a) for all a in Q s.t vr(a) == k
    val collectedVvals =
    phase1bMessages.asScala.filter(phase1bMessage => phase1bMessage.vrnd == maxVrndSoFar)
      .filter(r => r.vval.size() > 0).map(r => r.vval).asJava

    val setOfCollectedVvals
      = new util.HashSet[util.List[InetAddressAndPort]](collectedVvals)

    var chosenProposal: util.List[InetAddressAndPort] = null
    // If V has a single element, then choose v.
    if (setOfCollectedVvals.size == 1)
    chosenProposal = setOfCollectedVvals.iterator.next()
    else { // if i-quorum Q of acceptors respond, and there is a k-quorum R such that vrnd = k and vval = v,
      // for all a in intersection(R, Q) -> then choose "v". When choosing E = N/4 and F = N/2, then
      // R intersection Q is N/4 -- meaning if there are more than N/4 identical votes.
      if (collectedVvals.size > 1) { // multiple values were proposed, so we need to check if there is a majority with the same value.
        val counters = new util.HashMap[util.List[InetAddressAndPort], Integer]
        Breaks.breakable {
          for (value <- setOfCollectedVvals.asScala) {
            if (!counters.containsKey(value)) counters.put(value, 0)
            val count = counters.get(value)
            if (count + 1 > (N / 4)) {
              chosenProposal = value
              Breaks.break //todo: break is not supported

            }
            else counters.put(value, count + 1)
          }
        }
      }
      setOfCollectedVvals
    }
    // At this point, no value has been selected yet and it is safe for the coordinator to pick any proposed value.
    // If none of the 'vvals' contain valid values (are all empty lists), then this method returns an empty
    // list. This can happen because a quorum of acceptors that did not vote in prior rounds may have responded
    // to the coordinator first. This is safe to do here for two reasons:
    //      1) The coordinator will only proceed with phase 2 if it has a valid vote.
    //      2) It is likely that the coordinator (itself being an acceptor) is the only one with a valid vval,
    //         and has not heard a Phase1bMessage from itself yet. Once that arrives, phase1b will be triggered
    //         again.
    //
    if (chosenProposal == null) {
      chosenProposal = phase1bMessages.asScala
        .filter((r: Phase1bMessage) => r.vval.size() > 0)
        .flatMap(r => r.vval.asScala).asJava

      trace(s"Proposing new value -- chosen:${chosenProposal}, list:${collectedVvals}, vrnd:${maxVrndSoFar}")
    }
    chosenProposal
  }


  /**
   * Primary ordering is by round number, and secondary ordering by the ID of the node that initiated the round.
   */
  def compareRanks(left: Rank, right: Rank): Int = {
    val compRound = Integer.compare(left.round, right.round)
    if (compRound == 0) return Integer.compare(left.nodeIndex, right.nodeIndex)
    compRound
  }

  private def broadcast(message: Phase1aMessage) = {
    val response = RequestOrResponse(RapidMessages.phase1aMessage, JsonSerDes.serialize(message), 0)
    peers.asScala.foreach(addr => {
      socketClient.sendOneWay(response, addr)
    })
  }
}
