package org.dist.consensus.zab

import java.io.{DataInputStream, InputStream}

class BinaryInputArchive(val is: InputStream) {
  def readByte() = ds.readByte()

  def readString() = ds.readUTF()

  def readLong(tag: String) = ds.readLong()

  def readInt(tag:String ) = ds.readInt()

  val ds = new DataInputStream(is)

  def readRecord(): QuorumPacket = {
    val recordType = ds.readInt()
    val zxid = ds.readLong()
    val data = readBuffer()
    QuorumPacket(recordType, zxid, data)
  }

  def readBuffer() = {
    val dataSize = ds.readInt()
    val data = new Array[Byte](dataSize)
    ds.read(data)
    data
  }
}
