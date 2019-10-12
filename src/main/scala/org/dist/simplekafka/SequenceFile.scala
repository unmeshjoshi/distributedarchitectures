package org.dist.simplekafka

import java.io.{DataOutputStream, File, RandomAccessFile}
import java.util.HashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

class SequenceFile {
  def getAllOffSetsFrom(offset: Long) = offsetIndexes.keySet().asScala.filter(i ⇒ i >= offset)

  val keyIndexes = new HashMap[String, Long]()
  val offsetIndexes = new HashMap[Long, Long]()

  val offset = new AtomicInteger(0)

  abstract class AbstractWriter(var fileName: String) {

    def lastModified = {
      val file = new File(fileName)
      file.lastModified
    }
  }

  class Writer(fileName: String) extends AbstractWriter(fileName) {
    var lastWritePosition:Long = 0

    protected var file = init(fileName)

    def getCurrentPosition: Long = file.getFilePointer

    def seek(position: Long): Unit = {
      file.seek(position)
    }

    def append(key: String, message: String):Int = {
      append(key, message.getBytes())
    }
    def append(key: String, buffer: Array[Byte]): Int = {
      if (key == null) throw new IllegalArgumentException("Key cannot be NULL.")
      val keyIndex = lastWritePosition
      file.seek(keyIndex)
      file.writeUTF(key)
      val length = buffer.size
      file.writeInt(length)
      file.write(buffer, 0, length)
      file.getFD.sync()
      this.lastWritePosition = file.getFilePointer
      keyIndexes.put(key, keyIndex)
      val currentOffset = offset.incrementAndGet()
      offsetIndexes.put(currentOffset, keyIndex)
      currentOffset

    }

    def getIndexFor(key: String) = keyIndexes.get(key)

    def append(key: String, value: Long): Unit = {
      if (key == null) throw new IllegalArgumentException("Key cannot be NULL.")
      file.seek(file.getFilePointer)
      file.writeUTF(key)
      file.writeLong(value)
    }

    def getFileSize: Long = file.length

    protected def init(filename: String) = {
      val file = new File(filename)
      if (!file.exists) file.createNewFile
      new RandomAccessFile(file, "rw")
    }
  }

  class Reader(var fileName: String) {
    protected var file = init(fileName)

    def seekToOffset(offset:Long) = {
      file.seek(offset)
    }
    def seekToKeyPosition(key: String) = {
      val index = keyIndexes.get(key)
      file.seek(index)
    }

    /**
     * This method dumps the next key/value into the DataOuputStream
     * passed in.
     *
     * @param bufOut - DataOutputStream that needs to be filled.
     * @return total number of bytes read/considered
     */
    def next(bufOut: DataOutputStream): Long = {
      var bytesRead = -1L
      if (isEOF) return bytesRead
      val startPosition = file.getFilePointer
      val key = file.readUTF
      if (key != null) {
        /* write the key into buffer */ bufOut.writeUTF(key)
        val dataSize = file.readInt
        /* write data size into buffer */ bufOut.writeInt(dataSize)
        val data = new Array[Byte](dataSize)
        file.readFully(data)
        /* write the data into buffer */ bufOut.write(data)
        val endPosition = file.getFilePointer
        bytesRead = endPosition - startPosition
      }
      bytesRead
    }

    def isEOF: Boolean = getCurrentPosition == getEOF

    def getEOF() = file.length

    def getCurrentPosition = file.getFilePointer

    protected def init(filename: String) = {
      val file = new File(filename)
      if (!file.exists) file.createNewFile
      new RandomAccessFile(file, "rw")
    }
  }

}
