package org.dist.consensus.zab

import java.net.{DatagramPacket, DatagramSocket, InetSocketAddress}
import java.nio.ByteBuffer
import java.util.Random

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.common.Logging

import scala.jdk.CollectionConverters._

case class ElectionResult(vote:Vote, count:Int, winningVote:Vote, winningCount:Int)

class QuorumConnectionManager {
  def sendReceiveUdp(xid: Int, electionAddress: InetAddressAndPort): Option[Vote] = {
    val s = new DatagramSocket
    s.setSoTimeout(200)
    val requestBytes = new Array[Byte](4)
    val requestBuffer = ByteBuffer.wrap(requestBytes)

    val requestPacket = new DatagramPacket(requestBytes, requestBytes.length)
    val responseBytes = new Array[Byte](28)
    val responseBuffer = ByteBuffer.wrap(responseBytes)
    val responsePacket = new DatagramPacket(responseBytes, responseBytes.length)

    requestBuffer.putInt(xid)
    requestPacket.setLength(4)
    requestPacket.setSocketAddress(new InetSocketAddress(electionAddress.address, electionAddress.port))
    s.send(requestPacket)
    s.receive(responsePacket)
    val responseXid = responseBuffer.getInt
    if (xid == responseXid) {
      responseBuffer.getLong() //ignore
      Some(Vote(responseBuffer.getLong, responseBuffer.getLong))
    } else {
      None
    }
  }

}

class Elector extends Logging {
  def elect(votes: Map[InetAddressAndPort, Vote]): ElectionResult = {
    var result = ElectionResult(Vote(Long.MinValue, Long.MinValue), 0, Vote(Long.MinValue, Long.MinValue), 0)
    val voteCounts = votes.values.groupBy(identity).mapValues(_.size)
    val max: (Vote, Int) = voteCounts.maxBy(tuple ⇒ tuple._2)

    votes.values.foreach(v ⇒ {
        if (v.zxid > result.vote.zxid || (v.zxid == result.vote.zxid && v.id > result.vote.id)) {
          result = result.copy(vote = v, count = 1)
        }
    })
    result.copy(winningVote = max._1, winningCount = max._2)
  }
}

class LeaderElection(servers:List[QuorumServer], quorumConnectionManager: QuorumConnectionManager) {

  def lookForLeader(): Unit = {
    val votes = new java.util.HashMap[InetAddressAndPort, Vote]
    servers.foreach(server ⇒ {
      val xid = new Random().nextInt
      val maybeVote = quorumConnectionManager.sendReceiveUdp(xid, server.electionAddress)
      maybeVote.foreach(v ⇒ votes.put(server.electionAddress, v))
    })

    new Elector().elect(votes.asScala.toMap)
  }
}

class QuorumPeer(config:QuorumPeerConfig, quorumConnectionManager: QuorumConnectionManager) {
  private val myid = config.serverId
  private var state: ServerState.Value = ServerState.LOOKING
  private var currentVote = Vote(myid, getLastLoggedZxid)
  @volatile private var running = true

  def getPeerState: ServerState.Value = state

  def setPeerState(newState: ServerState.Value): Unit = {
    state = newState
  }


  def run() = {
    while (running) {
      state match {
        case ServerState.LOOKING ⇒ {
          try {
            new LeaderElection(config.servers, quorumConnectionManager).lookForLeader()
          } catch {
            case _ ⇒ state = ServerState.LOOKING
          }
        }
        case ServerState.LEADING ⇒
        case ServerState.FOLLOWING ⇒
      }
    }
  }

  object ServerState extends Enumeration {
    type ServerState = Value
    val LOOKING, FOLLOWING, LEADING = Value
  }

  def getLastLoggedZxid():Long = {
    0
  }
}
