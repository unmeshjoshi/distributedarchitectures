package org.dist.consensus.zab

import java.io._
import java.net.Socket
import java.util.concurrent.LinkedBlockingQueue

import org.dist.queue.common.Logging

import scala.util.control.Breaks

class FollowerHandler(followerSocket: Socket, leader: Leader) extends Thread with Logging {
  val oa = new BinaryOutputArchive(new BufferedOutputStream(followerSocket.getOutputStream))
  val ia = new BinaryInputArchive(new BufferedInputStream(followerSocket.getInputStream()))
  val proposalOfDeath = QuorumPacket(Leader.PROPOSAL, 0, Array[Byte]())
  val leaderLastZxid: Long = 0 //TODO
  private val queuedPackets = new LinkedBlockingQueue[QuorumPacket]

  def ping() = {
    val ping = new QuorumPacket(Leader.PING, leader.lastProposed, Array[Byte]())
    queuePacket(ping)
  }

  def queuePacket(p: QuorumPacket) = {
    queuedPackets.add(p);
  }

  def synced() = true //TODO

  override def run(): Unit = {
    try {

      new Thread() {
        override def run(): Unit = {
          Thread.currentThread.setName("Sender-" + followerSocket.getRemoteSocketAddress)
          try sendPackets()
          catch {
            case e: InterruptedException ⇒
              warn("Interrupted", e)
          }
        }
      }.start()

      val firstPacket = ia.readRecord()
      if (firstPacket.recordType != Leader.LASTZXID) {
        error("First packet " + firstPacket + " is not LASTZXID!")
        return
      }
      val peerLastZxid = firstPacket.zxid
      var packetToSend = Leader.SNAP
      var logTxns = true

      val leaderLastZxid = leader.startForwarding(this, peerLastZxid)
      val newLeaderQP = new QuorumPacket(Leader.NEWLEADER, leaderLastZxid)
      oa.writeRecord(newLeaderQP)

      while (true) {
        val responsePacket = ia.readRecord()
        trace(s"Received response from ${followerSocket} ${responsePacket}")
        responsePacket.recordType match {
          case Leader.ACK ⇒
            leader.processAck(responsePacket.zxid, followerSocket)
          case Leader.PING ⇒
            //ignore ping handler
          case _ ⇒
            info(s"Handling packet ${responsePacket}")

        }
      }
    } catch {
      case e: Exception ⇒ error(s"Error while handling followers ${e}")
    }
  }

  def sendPackets() = {
    Breaks.breakable {
      while (true) {
        val p = queuedPackets.take();
        if (p == proposalOfDeath) {
          // Packet of death!
          Breaks.break;
        }
        //      if (p.getType() == Leader.PING) traceMask = ZooTrace.SERVER_PING_TRACE_MASK
        //      ZooTrace.logQuorumPacket(LOG, traceMask, 'o', p);
        //
        try {
          trace(s"Sending packet ${p} from leader to ${followerSocket}")
          oa.writeRecord(p)

        } catch {
          case e: Exception ⇒ {
            error("unexpected exception e ")
            Breaks.break;
          }
        }
      }
    }
  }
}
