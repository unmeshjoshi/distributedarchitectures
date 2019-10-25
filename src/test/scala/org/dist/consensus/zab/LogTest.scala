package org.dist.consensus.zab

import java.io.{File, FileInputStream, FileOutputStream}

import org.dist.consensus.zab.api.ClientRequestOrResponse
import org.dist.queue.TestUtils
import org.scalatest.FunSuite

class LogTest extends FunSuite {
  test("Should write and read transactions to log") {
    val logDir = TestUtils.tempDir("zab-")
    val zxid = 1 << 32L
    val logFileName = "log." + java.lang.Long.toHexString(zxid)
    val logStream = new FileOutputStream(new File(logDir, logFileName))
    val logArchive = new BinaryOutputArchive(logStream)


    writeFewRequests(zxid, logArchive)

    val logReadStream = new FileInputStream(new File(logDir, logFileName))
    val logReadArchive = new BinaryInputArchive(logReadStream)
    val txn = logReadArchive.readBuffer()
    val endMarker = logReadArchive.readByte()
    assert(endMarker == 'B')
    

  }

  private def writeFewRequests(zxid: Int, logArchive: BinaryOutputArchive) = {
    for(i ← 1 to 10) {
      val request = Request(null, ClientRequestOrResponse.SetDataKey, 1, Array[Byte]())
      request.txnHeader = TxnHeader(request.sessionId, request.xid, zxid, System.currentTimeMillis(), OpsCode.setData)
      request.txn = SetDataTxn("path", request.data)
      logArchive.writeBuffer(request.serializeTxn(), "TxnEntry")
      logArchive.write(0x42.toByte, "EOR")
    }
  }
}
