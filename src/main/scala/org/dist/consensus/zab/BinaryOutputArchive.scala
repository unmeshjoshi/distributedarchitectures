package org.dist.consensus.zab

import java.io.{DataOutputStream, OutputStream}

class BinaryOutputArchive(val os: OutputStream) {
  def writeBuffer(data: Array[Byte], tag: String): Unit = {
    ds.writeInt(data.size)
    ds.write(data)
  }

  def writeString(path: String, tag: String) = {
    ds.writeUTF(path)
  }

  def writeInt(value: Int, tag: String) = {
    ds.writeInt(value)
  }

  def writeLong(value:Long, tag: String) = {
    ds.writeLong(value)
  }

  val ds = new DataOutputStream(os)

  def writeRecord(p: QuorumPacket): Unit = {
    ds.writeInt(p.recordType)
    ds.writeLong(p.zxid)
    writeBuffer(p.data, "data")
    ds.flush()
  }

  def write(ba:Array[Byte]) = {
    ds.write(ba)
  }

  def write(byte:Byte, tag:String) = {
    ds.write(byte)
  }

  def flush() = {
    ds.flush()
  }
}
