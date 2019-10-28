package org.dist.consensus.zab

import java.io.{File, FileOutputStream}
import java.util.Random

import org.dist.queue.common.Logging

class SynRequestProcessor(zks: ZookeeperServer,
                          nextProcessor: RequestProcessor) extends RequestProcessor with Logging {
  private val r = new Random(System.nanoTime)

  private var logCount = 0
  var logStream:FileOutputStream = null
  var logArchive:BinaryOutputArchive = null

  def run() = {
  }

  override def processRequest(request: Request): Unit = {
    val lastZxidSeen:Long = -1
    val hdr: TxnHeader = request.txnHeader
    if (hdr.zxid < lastZxidSeen) warn("Current zxid " + hdr.zxid + " is <= " + lastZxidSeen)
    val txn: SetDataTxn = request.txn
    if (logStream == null) {
      logStream = new FileOutputStream(new File(zks.dataLogDir(), zks.getLogName(hdr.zxid)))
      logArchive = new BinaryOutputArchive(logStream)
    }

    //serialize hdr
    //serialize txn
    logArchive.writeBuffer(request.serializeTxn(), "TxnEntry")
    logArchive.write(0x42.toByte, "EOR")

    //TODO take snapshot after specified no. of log entries
    //TODO: Batch sync instead of syncing each time.
    logStream.getFD.sync()
    logCount += 1
    nextProcessor.processRequest(request)
  }
}
