package org.dist.consensus.zab

import org.apache.zookeeper.server.SyncRequestProcessor

object OpsCode {
  val getData = 4
  val setData = 5
}

object TxnHeader {
  @throws[java.io.IOException]
  def deserialize(a:BinaryInputArchive, tag: String) = {
    val cxid = a.readLong("cxid")
    val zxid = a.readLong("zxid")
    val time = a.readLong("time")
    val opsCode = a.readInt("opsCode")
    TxnHeader(0, cxid, zxid, time, opsCode)
  }
}

case class TxnHeader(sessionId: Long, cxid: Long, zxid: Long, time: Long, opsCode: Int) {
  @throws[java.io.IOException]
  def serialize(a:BinaryOutputArchive, tag: String): Unit = {
    a.writeLong(cxid, "cxid")
    a.writeLong(zxid, "zxid")
    a.writeLong(time, "time")
    a.writeInt(opsCode, "opsCode")
  }
}

object SetDataTxn {
  @throws[java.io.IOException]
  def deserialize(a:BinaryInputArchive, tag: String) = {
    val path =  a.readString()
    val data = a.readBuffer()
    SetDataTxn(path, data)
  }
}
case class SetDataTxn(path:String, data:Array[Byte], version:Int = 0) {
  @throws[java.io.IOException]
  def serialize(a:BinaryOutputArchive, tag: String): Unit = {
    a.writeString(path, "path")
    a.writeBuffer(data, "data")
  }
}

trait RequestProcessor {
  def processRequest(request:Request)
}

class AckProcessor(leader:Leader) extends RequestProcessor {
  override def processRequest(request: Request): Unit = {
      leader.processAck(request.txnHeader.zxid, null)
  }
}

class CommitProcessor() extends RequestProcessor {
  def commit(request: Option[Request]) = {}

  override def processRequest(request: Request): Unit = {}
}

class ProposalRequestProcessor(val zks:ZookeeperServer, nextProcessor:RequestProcessor) extends RequestProcessor {
  val syncProcessor = new SynRequestProcessor(zks, new AckProcessor(zks.leader))
  override def processRequest(request: Request): Unit = {
    //propose to all the followers and log and ack itself
    zks.leader.propose(request)
    syncProcessor.processRequest(request)
  }
}

class PrepRequestProcessor(val zks:ZookeeperServer, nextProcessor:RequestProcessor) {

  def pRequest (request: Request) = {
    val txnHeader = TxnHeader(request.sessionId, request.xid, zks.getNextZxid(), zks.getTime(), OpsCode.setData)
    val txn = SetDataTxn("path", request.data)
    request.txn = txn
    request.txnHeader = txnHeader
    nextProcessor.processRequest(request)
  }
}
