package org.dist.consensus.zab

import java.net.{DatagramPacket, DatagramSocket, InetSocketAddress}
import java.nio.ByteBuffer
import java.util.Random

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.common.Logging

import scala.jdk.CollectionConverters._
import scala.util.control.Breaks._

case class ElectionResult(vote:Vote, count:Int, winningVote:Vote, winningCount:Int, noOfServers:Int) {
  def isElected() = {
    winningCount > (noOfServers / 2)
  }
}

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

object ServerState extends Enumeration {
  type ServerState = Value
  val LOOKING, FOLLOWING, LEADING = Value
}

class Elector(noOfServers:Int) extends Logging {
  def elect(votes: Map[InetAddressAndPort, Vote]): ElectionResult = {
    var result = ElectionResult(Vote(Long.MinValue, Long.MinValue), 0, Vote(Long.MinValue, Long.MinValue), 0, noOfServers)
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

class LeaderElection(servers:List[QuorumServer], quorumConnectionManager: QuorumConnectionManager, quorumPeer: QuorumPeer) extends Logging {

  def lookForLeader() = {
    breakable {

      while (true) {
        val votes = getVotesFromPeers
        val electionResult = new Elector(servers.size).elect(votes.asScala.toMap)

        if (electionResult.isElected()) {
          setLeaderOrFollowerState(electionResult)

          Thread.sleep(100) //TODO: Why?
          break

        } else {

          setCurrentVoteToWouldBeLeaderVote(electionResult)
        }

        info("Waiting for leader to be selected: " + votes)
        Thread.sleep(1000)
      }
    }
  }

  private def setCurrentVoteToWouldBeLeaderVote(electionResult: ElectionResult) = {
    quorumPeer.currentVote = electionResult.vote
  }

  private def getVotesFromPeers = {
    val votes = new java.util.HashMap[InetAddressAndPort, Vote]
    servers.foreach(server ⇒ {
      val xid = new Random().nextInt
      val maybeVote = quorumConnectionManager.sendReceiveUdp(xid, server.electionAddress)
      maybeVote.foreach(v ⇒ votes.put(server.electionAddress, v))
    })
    votes
  }

  private def setLeaderOrFollowerState(electionResult: ElectionResult) = {
    //set state as leader
    quorumPeer.currentVote = electionResult.winningVote
    if (electionResult.winningVote.id == quorumPeer.myid) {
      info(s"Setting ${electionResult.winningVote.id} to be leader")
      quorumPeer.setPeerState(ServerState.LEADING)
    } else {
      info(s"Setting ${quorumPeer.myid} to be follower of ${electionResult.winningVote.id}")
      quorumPeer.setPeerState(ServerState.FOLLOWING)
    }
  }
}

class ResponderThread(quorumPeer: QuorumPeer) extends Thread("ResponderThread") with Logging {
  override def run(): Unit = {
    try {
      val config = quorumPeer.config
      val udpSocket = new DatagramSocket(config.electionAddress.port, config.electionAddress.address)
      val b = new Array[Byte](36)
      val responseBuffer = ByteBuffer.wrap(b)
      val packet = new DatagramPacket(b, b.length)
      breakable {
        while (true) {
          udpSocket.receive(packet)
          if (packet.getLength != 4) {
            warn("Got more than just an xid! Len = " + packet.getLength)

          } else {
            responseBuffer.clear
            responseBuffer.getInt // Skip the xid

            responseBuffer.putLong(quorumPeer.myid)
            quorumPeer.state match {
              case ServerState.LOOKING ⇒
                info(this.getId + " Sending vote " + quorumPeer.currentVote)
                responseBuffer.putLong(quorumPeer.currentVote.id)
                responseBuffer.putLong(quorumPeer.currentVote.zxid)
            }
            packet.setData(b)
            udpSocket.send(packet)
          }
          packet.setLength(b.length)
        }
      }
    }  catch {
      case e:Exception ⇒ {
        e.printStackTrace()
      }
    }
  }

}

class QuorumPeer(val config:QuorumPeerConfig, quorumConnectionManager: QuorumConnectionManager) extends Thread with Logging {
  val myid = config.serverId
  @volatile var state: ServerState.Value = ServerState.LOOKING
  @volatile var currentVote = Vote(myid, getLastLoggedZxid)
  @volatile private var running = true

  new ResponderThread(this).start()

  def getPeerState: ServerState.Value = state

  def setPeerState(newState: ServerState.Value): Unit = {
    state = newState
  }


  override def run() = {
    while (running) {
      state match {
        case ServerState.LOOKING ⇒ {
          try {
            val electionResult = new LeaderElection(config.servers, quorumConnectionManager, this).lookForLeader()

          } catch {
            case e:Exception ⇒ {
              e.printStackTrace()
              state = ServerState.LOOKING
            }
          }
        }
        case ServerState.LEADING ⇒ {
          info(s"${myid} Leading now")
          val leader = new Leader(this)
        }
        case ServerState.FOLLOWING ⇒ {
          info(s"${myid} Following now")
        }
      }
    }
  }

  def getLastLoggedZxid():Long = {
    0
  }
}
