package org.dist.consensus.zab

case class QuorumPacket(val recordType: Int, var zxid: Long, val data: Array[Byte]) {
  def this(recordType:Int, zxid:Long) {
    this(recordType, zxid, Array[Byte]())
  }
}
