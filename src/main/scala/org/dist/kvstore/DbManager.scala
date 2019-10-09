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
  val systemTable = new Table(metadataDirectory, "system")

  def start(localEndpoint:InetAddressAndPort): StorageMetadata = {
    val rowOpt: Option[Row] = systemTable.get(localEndpoint.toString)
    rowOpt match {
      case Some(row) =>{
        val newGeneration = row.value.toInt + 1
        val existingToken = row.key
        systemTable.writer.append(existingToken, s"${newGeneration}".getBytes)

        StorageMetadata(row.key, newGeneration)
      }
      case None => {
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
}
