package org.dist.consensus.zab

import java.io.{BufferedInputStream, BufferedOutputStream}
import java.net.Socket
import java.util.concurrent.LinkedBlockingQueue

import org.dist.queue.common.Logging

import scala.util.control.Breaks

class FollowerHandler(peerSocket: Socket, leader: Leader) extends Thread with Logging {
  private val queuedPackets = new LinkedBlockingQueue[QuorumPacket]

  val proposalOfDeath = QuorumPacket(Leader.PROPOSAL, 0, Array[Byte]())

  val leaderLastZxid: Long = 0 //TODO

  override def run(): Unit = {
    val os = new BufferedOutputStream(peerSocket.getOutputStream)
    val is = new BufferedInputStream(peerSocket.getInputStream())
    val newLeaderQP = new QuorumPacket(Leader.NEWLEADER, leaderLastZxid,  Array[Byte]())

    new Thread() {
      override def run(): Unit = {
        Thread.currentThread.setName("Sender-" + peerSocket.getRemoteSocketAddress)
        try sendPackets()
        catch {
          case e: InterruptedException ⇒
            warn("Interrupted", e)
        }
      }
    }.start()
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
        //      try {
        //        oa.writeRecord(p, "packet");
        //        bufferedOutput.flush();
        //      } catch (IOException e) {
        //        if (!s.isClosed()) {
        //          LOG.warn("Unexpected exception", e);
        //        }
        //        break;
        //      }
        //    }
      }
    }

  }
}
