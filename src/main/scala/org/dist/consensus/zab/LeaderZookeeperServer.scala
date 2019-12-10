package org.dist.consensus.zab

import java.net.Socket

import org.dist.consensus.zab.api.ClientRequestOrResponse

trait ZookeeperServer {
  val dataTree = new DataTree
  def setupRequestProcessors(): Unit
  def config():QuorumPeerConfig
  def dataLogDir() = config().dataDir
  def getLogName(zxid: Long): String = "log." + java.lang.Long.toHexString(zxid)
}


class LeaderZookeeperServer(val leader:Leader) extends ZookeeperServer {
  def submitRequest(clientRequest: ClientRequestOrResponse, socket: Socket): Any = {
    val json = clientRequest.messageBodyJson
    val request = Request(socket, clientRequest.requestId, clientRequest.correlationId, json.getBytes)
    firstProcessor.processRequest(request)
  }

  var firstProcessor:RequestProcessor = null
  def setupRequestProcessors(): Unit = {
    val finalProcessor = new FinalRequestProcessor()
    val syncProcessor = new ProposalRequestProcessor(this, finalProcessor)
    firstProcessor = new PrepRequestProcessor(this, syncProcessor)
    firstProcessor.asInstanceOf[PrepRequestProcessor].start()
  }

  val commitProcessor = new CommitProcessor(this)

  def getNextZxid() = getZxid() + 1

  def getTime() = System.currentTimeMillis

  def getZxid() = hzxid

  protected var hzxid = 0L



  def setZxid(zxid: Long) = {
    this.hzxid = zxid
  }

  override def config(): QuorumPeerConfig = leader.config
}
