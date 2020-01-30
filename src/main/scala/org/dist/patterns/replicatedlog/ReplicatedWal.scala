package org.dist.patterns.replicatedlog

import java.io.File

import org.dist.patterns.wal.{Wal, WalEntry}

class ReplicatedWal(walDir:File) {
  def entries(from: Long, to: Long) = {
    wal.entries(from, to)
  }

  val wal = Wal.create(walDir)

  var highWaterMark = 0L

  def readAll() = wal.readAll()

  def lastLogEntryId = wal.lastLogEntryId

  def isUptoDate(entryId:Long) = {
    wal.lastLogEntryId == entryId
  }

  def truncate(entryId:Long) = {
    wal.truncate(entryId)
  }

  def append(bytes: Array[Byte]):Long = {
    wal.writeEntry(bytes)
  }

  def updateHighWaterMark(entryId:Long) = {
    highWaterMark = entryId
  }
}
