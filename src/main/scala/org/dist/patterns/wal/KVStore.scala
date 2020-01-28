package org.dist.patterns.wal

import java.io.{ByteArrayInputStream, File}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class KVStore(walDir:File) {

  def get(key: String): Option[String] = kv.get(key)

  val kv = new mutable.HashMap[String, String]()
  val wal = Wal.create(walDir)

  def applyLog() = {
    val entries: ListBuffer[WalEntry] = wal.readAll()
    entries.foreach(entry ⇒ {
      val command = SetValueCommand.deserialize(new ByteArrayInputStream(entry.data))
      kv.put(command.key, command.value
      )
    })
  }

  def close = {
    wal.close()
    kv.clear()
  }

  def put(key:String, value:String): Unit = {
    wal.writeEntry(SetValueCommand(key, value).serialize())

    kv.put(key, value)
  }
}
