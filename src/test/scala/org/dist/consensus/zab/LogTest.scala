package org.dist.consensus.zab

import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream}

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


    writeFewRequests(zxid, logArchive, 10)

    val logReadStream = new FileInputStream(new File(logDir, logFileName))
    val logReadArchive = new BinaryInputArchive(logReadStream)
    for (i ← 1 to 10) {
      val txn = logReadArchive.readBuffer()
      val endMarker = logReadArchive.readByte()
      val txnHeaderAndTxn = Request.deserializeTxn(new ByteArrayInputStream(txn))
      assert(txnHeaderAndTxn._2.path == s"/path/${i}")
      assert(txnHeaderAndTxn._2.data.sameElements(s"Value${i}".getBytes))
    }
  }

  private def writeFewRequests(zxid: Int, logArchive: BinaryOutputArchive, noOfRequests: Int) = {
    for (i ← 1 to noOfRequests) {
      val value = s"Value${i}"
      val request = Request(null, ClientRequestOrResponse.SetDataKey, 1, value.getBytes())
      request.txnHeader = TxnHeader(request.sessionId, request.xid, zxid, System.currentTimeMillis(), OpsCode.setData)
      request.txn = SetDataTxn(s"/path/${i}", request.data)
      logArchive.writeBuffer(request.serializeTxn(), "TxnEntry")
      logArchive.write(0x42.toByte, "EOR")
    }
  }
}
