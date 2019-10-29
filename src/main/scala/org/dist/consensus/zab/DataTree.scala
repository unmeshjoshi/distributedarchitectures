package org.dist.consensus.zab

import java.util

class DataTree {
  var lastProcessedZxid: Long = 0
  val nodes = new util.HashMap[String, Array[Byte]]()

  def processTransaction(txnHeader: TxnHeader, txn: SetDataTxn) = {
    val opsCode = txnHeader.opsCode
    opsCode match {
      case OpsCode.setData ⇒ {
        nodes.put(txn.path, txn.data)
      }
    }
  }

}
