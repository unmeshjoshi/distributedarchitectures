package org.dist.patterns.distlog

import java.nio.ByteBuffer

import org.dist.queue.TestUtils
import org.scalatest.FunSuite

class BookieReadWriteTest extends FunSuite {

  test("Write and Read to Ledger") {
    val bookie = new Bookie(TestUtils.tempDir("bookie"), TestUtils.tempDir("ledger"), TestUtils.tempDir("cache"))
    val ledgerId = 1
    val entryId = 1
    val content = 1
    bookie.addEntry(ledgerId, createByteBuffer(content, ledgerId, entryId));
    val buffer = bookie.readEntry(ledgerId, entryId)
    val a = buffer.getLong()
    val b = buffer.getLong()
    assert(buffer.getInt() == content)
  }

  private def createByteBuffer(i: Int, lid: Long, eid: Long) = {
    val bb = ByteBuffer.allocate(4 + 16)
    bb.putLong(lid)
    bb.putLong(eid)
    bb.putInt(i)
    bb.flip
    bb
  }
}
