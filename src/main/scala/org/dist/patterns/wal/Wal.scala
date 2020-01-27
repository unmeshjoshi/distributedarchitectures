package org.dist.patterns.wal

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel

object Wal {
  val logSuffix = ".log"
  val logPrefix = "wal"
  val firstLogId = 0

  def create(walDir:File): Wal = {
    val newLogFile = new File(walDir, logFileName())
    val channel = new RandomAccessFile(newLogFile, "rw").getChannel
    new Wal(channel)
  }

  def logFileName() = s"${logPrefix}-${firstLogId}${logSuffix}"
}

class Wal(logChannel:FileChannel) {
  def writeEntry(logEntry:LogEntry) = {
    val entrySize = logEntry.data.length + 16 //4 bytes for record length + 4 bytes for type + 8 bytes entry id + data size
    val buffer = newBuffer(entrySize)
    buffer.clear()
    buffer.putInt(entrySize - 4)
    buffer.putInt(0) //normal entry
    buffer.putLong(logEntry.entryId) //entryId
    buffer.put(logEntry.data)
    buffer.flip()
    while (buffer.hasRemaining) {
      logChannel.write(buffer)
    }
    logChannel.force(true)
    logChannel.position()
    println(s"Wrote ${logChannel.size()}")
  }

  import java.nio.ByteBuffer
  def newBuffer(size:Int) = {
    val buf = ByteBuffer.allocate(size)
    buf.clear
  }

  def close() = logChannel.close()


  def readAll() = {
    val entries = new scala.collection.mutable.ListBuffer[LogEntry]
    var bytesRead = 0L
    while(bytesRead < logChannel.size()) {
      val entryLengthFieldSize = 4
      val entrySizeBuffer = readFromChannel(entryLengthFieldSize)
      entrySizeBuffer.flip()
      val entrySize = entrySizeBuffer.getInt()

      val entryTypeBuffer = readFromChannel(4)
      entryTypeBuffer.flip()
      val entryType = entryTypeBuffer.getInt()

      val entryIdBuffer = readFromChannel(8)
      entryIdBuffer.flip()
      val entryId = entryIdBuffer.getLong()
      val dataSize = entrySize - ( 4 + 8)

      val walEntryData = readFromChannel(dataSize)
      walEntryData.flip()
      entries += LogEntry(entryId, walEntryData.array())
      bytesRead  = bytesRead + entrySize + entryLengthFieldSize
    }
    entries
  }

  private def readFromChannel(size:Int) = {
    val sizeBuffer = newBuffer(size)
    sizeBuffer.clear()
    while (logChannel.read(sizeBuffer) > 0) {}
    sizeBuffer
  }


}
