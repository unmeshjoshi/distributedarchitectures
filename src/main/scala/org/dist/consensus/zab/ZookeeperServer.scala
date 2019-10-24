package org.dist.consensus.zab

import javax.xml.crypto.Data

class ZookeeperServer(val leader:Leader) {
  val commitProcessor = new CommitProcessor()

  def dataLogDir() = leader.config.dataDir

  def getLogName(zxid: Long): String = "log." + java.lang.Long.toHexString(zxid)

  def getNextZxid() = getZxid() + 1

  def getTime() = System.currentTimeMillis

  def getZxid() = hzxid

  protected var hzxid = 0L

  val dataTree = new DataTree

  def setZxid(zxid: Long) = {
    this.hzxid = zxid
  }

}
