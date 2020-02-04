package org.dist.patterns.wal

import org.dist.queue.TestUtils
import org.scalatest.FunSuite

import scala.collection.mutable

class WalTest extends FunSuite {

  test("should write and read log entries from wal") {
    val walDir = TestUtils.tempDir("waltest")
    val wal = Wal.create(walDir)
    wal.writeEntry("test content".getBytes())
    wal.writeEntry("test content2".getBytes())
    wal.close()

    val readWal = Wal.create(walDir)
    val entries: mutable.Seq[WalEntry] = readWal.readAll()
    assert(2 == entries.size)
    assert(1 == entries(0).entryId)
    assert("test content" == new String(entries(0).data))
    assert(2 == entries(1).entryId)
    assert("test content2" == new String(entries(1).data))
  }

  test("should be able to truncate wal at a specific entry Id") {
    val walDir = TestUtils.tempDir("waltest")
    val wal = Wal.create(walDir)
    wal.writeEntry("test content".getBytes())
    wal.writeEntry("test content2".getBytes())
    wal.writeEntry("test content3".getBytes())
    wal.close()

    val readWal = Wal.create(walDir)
    assert(3 == readWal.readAll().size)

    readWal.truncate(2)
    val entries: mutable.Seq[WalEntry] = readWal.readAll()
    assert(2 == entries.size)

    assert("test content" == new String(entries(0).data))
    assert("test content2" == new String(entries(1).data))

    readWal.truncate(1)
    val entries2: mutable.Seq[WalEntry] = readWal.readAll()
    assert(1 == entries2.size)
    assert("test content" == new String(entries2(0).data))
  }
}
