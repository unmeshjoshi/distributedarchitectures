package org.dist.consensus.zab

import org.dist.queue.common.Logging

class FollowerZookeeperServer(follower:FollowerS) extends Logging {
  def commit(): Unit = {
    info(s"Committing from ${follower.self.config.serverId}")
  }

  def logRequest(hdr:TxnHeader, txn: SetDataTxn): Unit = {
    info(s"Logging ${hdr} and ${txn} from ${follower.self.config.serverId}")
  }
}
