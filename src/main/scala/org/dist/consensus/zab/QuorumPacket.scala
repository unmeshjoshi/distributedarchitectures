package org.dist.consensus.zab

case class QuorumPacket(val recordType: Int, val zxid: Long, val data: Array[Byte])