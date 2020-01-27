package org.dist.patterns.wal

import org.dist.queue.TestUtils
import org.scalatest.FunSuite

import scala.collection.mutable

class WalTest extends FunSuite {

  test("should write and read log entries to wal") {
    val walDir = TestUtils.tempDir("waltest")
    val wal = Wal.create(walDir)
    wal.writeEntry(LogEntry(1, "test content".getBytes()))
    wal.writeEntry(LogEntry(2, "test content2".getBytes()))
    wal.close()

    val readWal = Wal.create(walDir)
    val entries: mutable.Seq[LogEntry] = readWal.readAll()
    assert(2 == entries.size)
    assert(1 == entries(0).entryId)
    assert("test content" == new String(entries(0).data))
    assert(2 == entries(1).entryId)
    assert("test content2" == new String(entries(1).data))
  }
}
