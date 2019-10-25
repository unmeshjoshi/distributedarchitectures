package org.dist.consensus.zab

import java.net.Socket

import org.dist.consensus.zab.api.ClientRequestOrResponse


class ZookeeperServer(val leader:Leader) {
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
  }

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
