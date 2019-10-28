package org.dist.consensus.zab

import org.dist.consensus.zab.api.ClientRequestOrResponse
import org.dist.queue.common.Logging

class SendAckRequestProcessor(follower:FollowerS) extends RequestProcessor {
  override def processRequest(request: Request): Unit = {
    val qp = new QuorumPacket(Leader.ACK, request.txnHeader.zxid)
    follower.writePacket(qp)
  }
}

class FollowerZookeeperServer(follower:FollowerS) extends ZookeeperServer with Logging {
  val syncProcessor = new SynRequestProcessor(this, new SendAckRequestProcessor(follower))
  def commit(): Unit = {
    info(s"Committing from ${follower.self.config.serverId}")
  }

  def logRequest(hdr:TxnHeader, txn: SetDataTxn): Unit = {
    info(s"Logging ${hdr} and ${txn} from ${follower.self.config.serverId}")
    val request = new Request(null, ClientRequestOrResponse.SetDataKey, hdr.cxid, txn.data, 0, txn, hdr)
    syncProcessor.processRequest(request)
  }

  override def setupRequestProcessors(): Unit = {} //already done in the construction

  override def config(): QuorumPeerConfig = follower.self.config
}
