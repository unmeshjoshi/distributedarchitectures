package org.dist.kvstore

import org.dist.queue.TestUtils
import org.scalatest.FunSuite

class TableReadWriteTest extends FunSuite {

  test("should write and read rows from table") {
    val table = new Table(TestUtils.tempDir().getAbsolutePath, "Table1")
    table.writer.append("key1", "value1".getBytes)
    assert("value1" == table.get("key1").get.value)
  }


  test("should write multiple values and read") {
    val table = new Table(TestUtils.tempDir().getAbsolutePath, "Table1")
    table.writer.append("key1", "value1".getBytes)
    table.writer.append("key2", "value2".getBytes)
    table.writer.append("key3", "value3".getBytes)

    assert("value1" == table.get("key1").get.value)
    assert("value2" == table.get("key2").get.value)

    table.writer.append("key3", "value4".getBytes)
    assert("value4" == table.get("key3").get.value)
  }

}
