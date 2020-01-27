package org.dist.patterns.wal

import org.dist.queue.TestUtils
import org.scalatest.FunSuite

class KVStoreTest extends FunSuite {

  test("should append entries to wal") {
    val walDir = TestUtils.tempDir("waltest")
    val kv = new KVStore(walDir)
    kv.put("testKey", "testValue")

    val wal = Wal.create(walDir)
    val entries = wal.readAll()
    assert(1 == entries.size)
  }

  test("should initialize kv store from wal") {
    val walDir = TestUtils.tempDir("waltest")
    val kv = new KVStore(walDir)
    kv.put("testKey", "testValue")
    kv.put("testKey2", "testValue2")

    kv.close

    val kv2 = new KVStore(walDir)
    assert(Some("testValue") == kv2.get("testKey"))
    assert(Some("testValue2") == kv2.get("testKey2"))
    assert(kv2.lastLogEntry == 2)
  }

  test("should initialize lastLogIndex from wal") {
    val walDir = TestUtils.tempDir("waltest")
    val kv = new KVStore(walDir)
    kv.put("testKey", "testValue")
    kv.put("testKey2", "testValue2")

    kv.close

    val kv2 = new KVStore(walDir)
    assert(kv2.lastLogEntry == 2)
  }

  test("should increment lastLogEntry after every mutation") {
    val walDir = TestUtils.tempDir("waltest")
    val kv = new KVStore(walDir)
    kv.put("testKey", "testValue")
    assert(kv.lastLogEntry == 1)

    kv.put("testKey2", "testValue2")
    assert(kv.lastLogEntry == 2)
  }

}
