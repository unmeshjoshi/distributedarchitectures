package org.dist.consensus.zab

import java.io.ByteArrayInputStream

import org.scalatest.FunSuite

class TransactionSerializationTest extends FunSuite {

  test("should serialize and deserialize Transactions") {
    val request = Request(null, 1, 1, Array[Byte](2))
    request.txnHeader = TxnHeader(request.sessionId, request.xid, 1000, 1000, OpsCode.setData)
    request.txn = SetDataTxn("path", request.data)

    val bytes = request.serializeTxn()

    val tuple = Request.deserializeTxn(new ByteArrayInputStream(bytes))
    assert(tuple._1 == request.txnHeader)
    assert(tuple._2.path == request.txn.path)
    assert(tuple._2.version == request.txn.version)
    assert(tuple._2.data.sameElements(request.txn.data))
  }
}
