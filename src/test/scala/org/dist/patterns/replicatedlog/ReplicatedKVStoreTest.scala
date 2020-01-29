package org.dist.patterns.replicatedlog

import org.dist.patterns.wal.Wal
import org.dist.queue.TestUtils
import org.scalatest.FunSuite

class ReplicatedKVStoreTest extends FunSuite {

  test("Should be able to compare if two logs are uptodate") {
    val walDir = TestUtils.tempDir("waltest")
    val wal = new ReplicatedWal(walDir)
    wal.append("test content".getBytes())
    wal.append("test content2".getBytes())
    wal.append("test content3".getBytes())

    assert(wal.isUptoDate(3))
  }

  test("Should be able to truncate to specific index") {
    val walDir = TestUtils.tempDir("waltest")
    val wal = new ReplicatedWal(walDir)
    wal.append("test content".getBytes())
    wal.append("test content2".getBytes())
    wal.append("test content3".getBytes())

    wal.truncate(1)

    assert(1 == wal.readAll().size)
  }
}
