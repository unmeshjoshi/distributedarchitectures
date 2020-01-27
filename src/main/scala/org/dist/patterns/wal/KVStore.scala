package org.dist.patterns.wal

import java.io.{ByteArrayInputStream, File}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class KVStore(walDir:File) {
  def get(key: String): Option[String] = kv.get(key)

  val kv = new mutable.HashMap[String, String]()
  var lastLogEntry:Long = 0
  val wal = Wal.create(walDir)

  init()

  def init() = {
    val entries: ListBuffer[LogEntry] = wal.readAll()
    entries.foreach(entry ⇒ {
      lastLogEntry = entry.entryId
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
    val nextLogEntry = lastLogEntry + 1
    val logEntry = LogEntry(nextLogEntry, SetValueCommand(key, value).serialize())
    wal.writeEntry(logEntry)
    lastLogEntry = nextLogEntry

    kv.put(key, value)
  }
}
