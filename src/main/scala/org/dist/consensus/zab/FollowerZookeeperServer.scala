package org.dist.consensus.zab

import java.util.concurrent.LinkedBlockingQueue

import org.dist.consensus.zab.api.ClientRequestOrResponse
import org.dist.queue.common.Logging

class SendAckRequestProcessor(follower:FollowerS) extends RequestProcessor {
  override def processRequest(request: Request): Unit = {
    val qp = new QuorumPacket(Leader.ACK, request.txnHeader.zxid)
    follower.writePacket(qp)
  }
}

class FollowerZookeeperServer(follower:FollowerS) extends ZookeeperServer with Logging {
  val commitProcessor = new CommitProcessor(this)
  val syncProcessor = new SynRequestProcessor(this, new SendAckRequestProcessor(follower))
  val pendingTxns = new LinkedBlockingQueue[Request]()

  def commit(zxid:Long): Unit = {
    try {
      if (pendingTxns.size == 0) {
        warn("Committing " + zxid + " without seeing txn")
        return
      }
    info(s"Committing ${zxid} from ${follower.self.config.serverId}")
    val firstElementZxid = pendingTxns.element.txnHeader.zxid
    if (firstElementZxid != zxid) {
      error("Committing " + java.lang.Long.toHexString(zxid) + " but next pending txn " + java.lang.Long.toHexString(firstElementZxid))
      System.exit(12)
    }
    val request = pendingTxns.remove()
    commitProcessor.commit(request)
    } catch {
      case e:Exception ⇒ e.printStackTrace()
    }
  }

  def logRequest(hdr:TxnHeader, txn: SetDataTxn): Unit = {
    info(s"Logging ${hdr} and ${txn} from ${follower.self.config.serverId}")
    val request = new Request(null, ClientRequestOrResponse.SetDataKey, hdr.cxid, txn.data, 0, txn, hdr)
    if ((hdr.zxid & 0xffffffffL) != 0) {
      pendingTxns.add(request)
    }
    syncProcessor.processRequest(request)
  }

  override def setupRequestProcessors(): Unit = {} //already done in the construction

  override def config(): QuorumPeerConfig = follower.self.config
}
