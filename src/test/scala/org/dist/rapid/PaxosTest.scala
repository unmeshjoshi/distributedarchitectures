package org.dist.rapid

import java.util

import org.dist.kvstore.InetAddressAndPort
import org.dist.rapid.messages._
import org.scalatest.FunSuite

class PaxosTest extends FunSuite {

  class StubPaxosMessagingClient extends PaxosMessagingClient {
    val requests = new util.HashMap[InetAddressAndPort, Phase1bMessage]()
    var phase1aMessage:Phase1aMessage = _
    var phase2aMessage:Phase2aMessage = _
    var phase2bMessage:Phase2bMessage = _

    override def sendPhase1bMessage(message: Phase1bMessage, to: InetAddressAndPort): Unit = {
      requests.put(to, message)
    }

    override def broadcast(message: Phase1aMessage): Unit = {
      phase1aMessage = message
    }

    override def broadcast(message: Phase2aMessage): Unit = {
      phase2aMessage = message
    }

    override def broadcast(message: Phase2bMessage): Unit = {
      phase2bMessage = message
    }
  }

  test("should set round to proposed round if not started higher numbered round") {
    val address = InetAddressAndPort.create("10.10.10.10", 1009)
    val consensusOnAddress = new util.ArrayList[InetAddressAndPort]()
    val stubClient = new StubPaxosMessagingClient
    val paxos = new Paxos(address, 10, (list)=>{
      consensusOnAddress.addAll(list)
    }, stubClient)

    paxos.registerFastRoundVote(util.Arrays.asList(InetAddressAndPort.create("10.10.10.11", 1009)))

    val sender = InetAddressAndPort.create("10.10.10.20", 80)
    paxos.handlePhase1aMessage(Phase1aMessage(0, sender, Rank(2, 1)))

    assert(paxos.rnd == Rank(2, 1))
    val phase1bMessage = stubClient.requests.get(sender)
    val value = util.Arrays.asList(InetAddressAndPort.create("10.10.10.11", 1009))
    assert(phase1bMessage == Phase1bMessage(0, Rank(2, 1), address, Rank(1, 1), value))
  }

  test("should set value if accept (phase2a) message is received from higher round and value not already set for the round") {
    val address = InetAddressAndPort.create("10.10.10.10", 1009)
    val consensusOnAddress = new util.ArrayList[InetAddressAndPort]()
    val stubClient = new StubPaxosMessagingClient
    val paxos = new Paxos(address, 10, (list)=>{
      consensusOnAddress.addAll(list)
    }, stubClient)

    val proposedValue = util.Arrays.asList(InetAddressAndPort.create("10.10.10.11", 1009))

    val sender = InetAddressAndPort.create("10.10.10.20", 80)
    paxos.rnd = Rank(1, 0)
    paxos.handlePhase2aMessage(Phase2aMessage(0, sender, Rank(1, 0), proposedValue))

    assert(paxos.rnd == Rank(1, 0))
    assert(stubClient.phase2bMessage == Phase2bMessage(0, Rank(1, 0), address, proposedValue))
  }

  test("should ignore accept (phase2a) message if value already set for the round") {
    val address = InetAddressAndPort.create("10.10.10.10", 1009)
    val consensusOnAddress = new util.ArrayList[InetAddressAndPort]()
    val stubClient = new StubPaxosMessagingClient
    val paxos = new Paxos(address, 10, (list)=>{
      consensusOnAddress.addAll(list)
    }, stubClient)

    val proposedValue = util.Arrays.asList(InetAddressAndPort.create("10.10.10.11", 1009))

    val sender = InetAddressAndPort.create("10.10.10.20", 80)
    paxos.rnd = Rank(1, 0)
    paxos.vrnd = Rank(1, 0)
    paxos.vval = proposedValue
    paxos.handlePhase2aMessage(Phase2aMessage(0, sender, Rank(1, 0), proposedValue))

    assert(paxos.rnd == Rank(1, 0))
    assert(stubClient.phase2bMessage == null)
  }


  test("should chose proposed value if none of the acceptors") {
    val address = InetAddressAndPort.create("10.10.10.10", 1009)
    val consensusOnAddress = new util.ArrayList[InetAddressAndPort]()
    val stubClient = new StubPaxosMessagingClient
    val paxos = new Paxos(address, 10, (list)=>{
      consensusOnAddress.addAll(list)
    }, stubClient)

    val proposedValue = util.Arrays.asList(InetAddressAndPort.create("10.10.10.11", 1009))

    val sender = InetAddressAndPort.create("10.10.10.20", 80)
    paxos.rnd = Rank(1, 0)
    paxos.vrnd = Rank(1, 0)
    paxos.vval = proposedValue
    paxos.handlePhase1bMessage(Phase1bMessage(0, Rank(1, 0), sender, Rank(0, 0), util.Arrays.asList()))

    assert(paxos.rnd == Rank(1, 0))
    assert(stubClient.phase2bMessage == null)
  }
}
