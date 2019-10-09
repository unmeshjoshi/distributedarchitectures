package org.dist.kvstore

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, IOException}

import scala.util.{Failure, Success, Try}


class Table(metadataDirectory:String, table: String) {
  val filePath: String = getFilePath
  val file = new SequenceFile()
  val reader = new file.Reader(filePath)
  val writer = new file.Writer(filePath)

  /*
       * This is a one time thing and hence we do not need
       * any commit log related activity. Just write in an
       * atomic fashion to the underlying SequenceFile.
      */
  private def apply(row: Row): Unit = {
    val currentPos = writer.getCurrentPosition
    val ba = new ByteArrayOutputStream()
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

    reader.seekToKeyPosition(key)
    reader.next(baos)

    val bais = new DataInputStream(new ByteArrayInputStream(ba.toByteArray))
    Try(Row.deserialize(bais)) match {
      case Success(row) => Some(row)
      case Failure(exception) => None
    }
  }

  def getFilePath = metadataDirectory + System.getProperty("file.separator") + table + ".db"

}
