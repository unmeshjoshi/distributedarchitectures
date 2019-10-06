package org.dist.kvstore

import java.io.File
import java.nio.file.Paths

import org.scalatest.{BeforeAndAfter, FunSuite}

class DbManagerTest extends FunSuite with BeforeAndAfter {

  before {
    TestUtils.deleteTable("system")
  }

  after {
    TestUtils.deleteTable("system")
  }


  test("should generate new token and generation info if its not already saved") {
    val metadata = new DbManager(TestUtils.tmpDir.toString).start(InetAddressAndPort.create("127.7.7.1", 8888))
    assert(metadata.generation == 1)
    assert(metadata.token != null)
  }

  test("should use existing token and increment generation info if its already saved") {
    //start to create a file
    new DbManager(TestUtils.tmpDir.toString).start(InetAddressAndPort.create("127.7.7.1", 8888))

    val metadata = new DbManager(TestUtils.tmpDir.toString).start(InetAddressAndPort.create("127.7.7.1", 8888))
    assert(metadata.generation == 2)
    assert(metadata.token != null)
  }

}
