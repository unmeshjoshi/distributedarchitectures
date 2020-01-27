package org.dist.patterns.wal

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream, InputStream}

object SetValueCommand {
  def deserialize(is:InputStream) = {
    val daos = new DataInputStream(is)
    val key = daos.readUTF()
    val value = daos.readUTF()
    SetValueCommand(key, value)
  }
}

case class SetValueCommand(val key:String, val value:String) {
  def serialize() = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeUTF(key)
    dataStream.writeUTF(value)
    baos.toByteArray
  }
}
