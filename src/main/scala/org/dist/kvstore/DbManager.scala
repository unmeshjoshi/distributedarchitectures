package org.dist.kvstore

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, IOException}
import java.math.BigInteger

import scala.util.{Failure, Success, Try}

object Row {
  def serialize(row: Row, dos:DataOutputStream): Unit = {
    dos.writeUTF(row.key)
    dos.writeInt(row.value.getBytes().size)
    dos.write(row.value.getBytes) //TODO: as of now only supporting string writes.
  }

  def deserialize(dis: DataInputStream): Row = {
    val key = dis.readUTF()
    val dataSize = dis.readInt()
    val bytes = new Array[Byte](dataSize)
    dis.read(bytes)
    val value = new String(bytes) //TODO:As of now supporting only string values
    Row(key, value)
  }
}

case class Row(key: String, value: String)

class DbManager(metadataDirectory: String) {
  private val systemTable = new SystemTable("system")

  def start(localEndpoint:InetAddressAndPort): StorageMetadata = {
    val rowOpt: Option[Row] = systemTable.get(localEndpoint.toString)
    rowOpt match {
      case Some(row) ⇒{
        val newGeneration = row.value.toInt + 1
        val existingToken = row.key
        systemTable.writer.append(existingToken, s"${newGeneration}".getBytes)

        StorageMetadata(row.key, newGeneration)
      }
      case None ⇒ {
        val token = newToken()
        val generation = 1
        systemTable.writer.append(token, s"${generation}".getBytes)
        StorageMetadata(token, generation)
      }
    }

  }

  def newToken() = {
    val guid: String = GuidGenerator.guid
    var token: BigInteger = FBUtilities.hash(guid)
    if (token.signum == -1) token = token.multiply(BigInteger.valueOf(-1L))
    token.toString
  }

  class SystemTable(table: String) {
    val systemTable: String = getFileName
    val file = new SequenceFile()
    val reader = new file.Reader(systemTable)
    val writer = new file.Writer(systemTable)

    /*
         * This is a one time thing and hence we do not need
         * any commit log related activity. Just write in an
         * atomic fashion to the underlying SequenceFile.
        */
    private def apply(row: Row): Unit = {
      val file = getFileName
      val currentPos = writer.getCurrentPosition
      var ba = new ByteArrayOutputStream()
      val bufOut = new DataOutputStream(ba)
      Row.serialize(row, bufOut)
      try writer.append(row.key, ba.toByteArray)
      catch {
        case e: IOException =>
          writer.seek(currentPos)
          throw e
      }
    }

    def get(key: String): Option[Row] = {
      val ba = new ByteArrayOutputStream()
      val baos = new DataOutputStream(ba)
      reader.next(baos)

      val bais = new DataInputStream(new ByteArrayInputStream(ba.toByteArray))
      Try(Row.deserialize(bais)) match {
        case Success(row) ⇒ Some(row)
        case Failure(exception) ⇒ None
      }
    }

    private def getFileName = metadataDirectory + System.getProperty("file.separator") + table + ".db"

  }
}
