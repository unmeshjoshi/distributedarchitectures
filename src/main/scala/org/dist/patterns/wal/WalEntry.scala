package org.dist.patterns.wal

import java.nio.ByteBuffer
import java.nio.channels.FileChannel

class WalEntryDeserializer(logChannel: FileChannel) {
  val intBuffer = Wal.newBuffer(Wal.sizeOfInt)
  val longBuffer = Wal.newBuffer(Wal.sizeOfLong)

  def deserialize() = {
    val entrySize: Int = readInt
    val entryType: Int = readInt
    val entryId: Long = readLong
    val (walEntryData, position) = readData(entrySize)

    (WalEntry(entryId, walEntryData.array()), entrySize + Wal.sizeOfInt, position)
  }


  private def readData(entrySize: Int) = {
    val dataSize = entrySize - (Wal.sizeOfInt + Wal.sizeOfLong)
    val (walEntryData, position) = readFromChannel(Wal.newBuffer(dataSize))
    (walEntryData, position)
  }

  private def readLong = {
    val (entryIdBuffer, position) = readFromChannel(longBuffer)
    val entryId = entryIdBuffer.getLong()
    entryId
  }

  private def readInt = {
    val (entrySizeBuffer, position) = readFromChannel(intBuffer)
    val entrySize = entrySizeBuffer.getInt()
    entrySize
  }

  private def readFromChannel(buffer:ByteBuffer):(ByteBuffer, Long) = {
    buffer.clear()
    val filePosition = readFromChannel(logChannel, buffer)
    (buffer.flip(), filePosition)
  }

  private def readFromChannel(channel:FileChannel, buffer:ByteBuffer) = {
    while (logChannel.read(buffer) > 0) {}
    logChannel.position()
  }
}

case class WalEntry(entryId:Long, data:Array[Byte], entryType:Int = 0) {

  def serialize():ByteBuffer = {
    val bufferSize = entrySize + 4 //4 bytes for record length + walEntry size
    val buffer = Wal.newBuffer(bufferSize)
    buffer.clear()
    buffer.putInt(entrySize)
    buffer.putInt(0) //normal entry
    buffer.putLong(entryId)
    buffer.put(data)
  }

  def entrySize = {
    data.length + Wal.sizeOfLong + Wal.sizeOfInt //size of all the fields
  }
}



