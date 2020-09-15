package org.dist.patterns.distlog

import org.scalatest.FunSuite

class LedgerCacheTest extends FunSuite {
  test("put and get offset values") {
    val ledgerCache = new LedgerCache(org.dist.queue.TestUtils.tempDir("bookkeeper"))
    ledgerCache.putEntryOffset(1, 1, 1000)
    ledgerCache.flush()
    val offset = ledgerCache.getEntryOffset(1, 1)
    assert(offset == 1000)
  }
}
