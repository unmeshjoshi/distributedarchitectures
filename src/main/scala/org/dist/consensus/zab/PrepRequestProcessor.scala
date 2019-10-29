package org.dist.consensus.zab

import org.dist.consensus.zab.api.{ClientRequestOrResponse, SetDataRequest}
import org.dist.kvstore.JsonSerDes
import org.dist.queue.common.Logging

object OpsCode {
  val getData = 4
  val setData = 5
}

object TxnHeader {
  @throws[java.io.IOException]
  def deserialize(a: BinaryInputArchive, tag: String) = {
    val cxid = a.readLong("cxid")
    val zxid = a.readLong("zxid")
    val time = a.readLong("time")
    val opsCode = a.readInt("opsCode")
    TxnHeader(0, cxid, zxid, time, opsCode)
  }
}

case class TxnHeader(sessionId: Long, cxid: Long, zxid: Long, time: Long, opsCode: Int) {
  @throws[java.io.IOException]
  def serialize(a: BinaryOutputArchive, tag: String): Unit = {
    a.writeLong(cxid, "cxid")
    a.writeLong(zxid, "zxid")
    a.writeLong(time, "time")
    a.writeInt(opsCode, "opsCode")
  }
}

object SetDataTxn {
  @throws[java.io.IOException]
  def deserialize(a: BinaryInputArchive, tag: String) = {
    val path = a.readString()
    val data = a.readBuffer()
    SetDataTxn(path, data)
  }
}

case class SetDataTxn(path: String, data: Array[Byte], version: Int = 0) {
  @throws[java.io.IOException]
  def serialize(a: BinaryOutputArchive, tag: String): Unit = {
    a.writeString(path, "path")
    a.writeBuffer(data, "data")
  }
}

trait RequestProcessor {
  def processRequest(request: Request)
}

class AckProcessor(leader: Leader) extends RequestProcessor with Logging {
  override def processRequest(request: Request): Unit = {
    info(s"Sending ACK for ${request}")
    leader.processAck(request.txnHeader.zxid, null)
  }
}

class FinalRequestProcessor extends RequestProcessor with Logging {
  override def processRequest(request: Request): Unit = {
    info(s"Final processing ${request}")
  }
}

class CommitProcessor(zk: ZookeeperServer) extends RequestProcessor with Logging {
  def commit(request: Request) = {
    zk.dataTree.processTransaction(request.txnHeader, request.txn)
    info(s"Datatree is now => ${zk.dataTree.nodes} on ${zk.config().serverId}")
  }

  override def processRequest(request: Request): Unit = {

  }
}

class ProposalRequestProcessor(val zks: LeaderZookeeperServer, nextProcessor: RequestProcessor) extends RequestProcessor {
  val syncProcessor = new SynRequestProcessor(zks, new AckProcessor(zks.leader))

  override def processRequest(request: Request): Unit = {
    //propose to all the followers and log and ack itself
    zks.leader.propose(request)
    syncProcessor.processRequest(request)
  }
}

class PrepRequestProcessor(val zks: LeaderZookeeperServer, nextProcessor: RequestProcessor) extends RequestProcessor with Logging {
  override def processRequest(request: Request): Unit = {
    if (request.requestType == ClientRequestOrResponse.SetDataKey) {
      val setDataRequest = JsonSerDes.deserialize(request.data, classOf[SetDataRequest])
      pRequest(request, setDataRequest)
    }
    else
      error(s"Invalid request type ${request.requestType}")
  }

  def pRequest(request: Request, setDataRequest:SetDataRequest) = {
    val txnHeader = TxnHeader(request.sessionId, request.xid, zks.getNextZxid(), zks.getTime(), OpsCode.setData)
    val txn = SetDataTxn(setDataRequest.path, setDataRequest.data.getBytes())
    request.txn = txn
    request.txnHeader = txnHeader
    nextProcessor.processRequest(request)
  }
}
