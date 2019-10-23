package org.dist.consensus.zab

import javax.xml.crypto.Data

class LeaderZookeeperServer(self:QuorumPeer) {
  def getZxid() = hzxid

  protected var hzxid = 0L

  val dataTree = new DataTree

  def setZxid(zxid: Long) = {
    this.hzxid = zxid
  }

}
