package org.dist.consensus.zab

import java.net.ServerSocket
import java.util

import org.dist.queue.common.Logging

import scala.jdk.CollectionConverters._

object Leader {
  /**
   * This message is for follower to expect diff
   */
  private[zab] val DIFF = 13

  /**
   * This is for follower to truncate its logs
   */
  private[zab] val TRUNC = 14

  /**
   * This is for follower to download the snapshots
   */
  private[zab] val SNAP = 15

  /**
   * This message type is sent by the leader to indicate it's zxid and if
   * needed, its database.
   */
  private[zab] val NEWLEADER = 10

  /**
   * This message type is sent by a follower to indicate the last zxid in its
   * log.
   */
  private[zab] val LASTZXID = 11

  /**
   * This message type is sent by the leader to indicate that the follower is
   * now uptodate andt can start responding to clients.
   */
  private[zab] val UPTODATE = 12

  /**
   * This message type is sent to a leader to request and mutation operation.
   * The payload will consist of a request header followed by a request.
   */
  private[zab] val REQUEST = 1

  /**
   * This message type is sent by a leader to propose a mutation.
   */
  val PROPOSAL = 2

  /**
   * This message type is sent by a follower after it has synced a proposal.
   */
  private[zab] val ACK = 3

  /**
   * This message type is sent by a leader to commit a proposal and cause
   * followers to start serving the corresponding data.
   */
  private[zab] val COMMIT = 4

  /**
   * This message type is enchanged between follower and leader (initiated by
   * follower) to determine liveliness.
   */
  private[zab] val PING = 5

  /**
   * This message type is to validate a session that should be active.
   */
  private[zab] val REVALIDATE = 6

  /**
   * This message is a reply to a synchronize command flushing the pipe
   * between the leader and the follower.
   */
  private[zab] val SYNC = 7
}

class Leader(self:QuorumPeer) extends Logging {
  val lastProposed: Long = 0
  val config = self.config

  val ss = new ServerSocket(config.serverAddress.port, 100, config.serverAddress.address)
  val followers = new util.ArrayList[FollowerHandler]()

  def lead() = {
    //setup tcpip server communication
    //set processing pipeline
    new Thread() {
      override def run(): Unit = {
        try {
          info(s"Leader listening on ${config.serverAddress.address} and ${config.serverAddress.port}")
          while (true) {
            val s = ss.accept
            s.setSoTimeout(config.tickTime * config.syncLimit)
            s.setTcpNoDelay(true)
            val followerHandler = new FollowerHandler(s, Leader.this)
            followers.add(followerHandler)
            followerHandler.start()
          }
        } catch {
          case e: Exception ⇒ {
            error("Error in follower connection. Moving ahead")
          }
          //
        }
      }
    }.start()

    while(true) {
      Thread.sleep(self.config.tickTime / 2)

      self.tick.incrementAndGet()

      followers.asScala.foreach(follower ⇒ {
        follower.ping()
      })
    }
  }

}
