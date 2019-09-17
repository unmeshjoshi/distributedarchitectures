package org.dist.kvstore

import java.io.File
import java.nio.file.Paths

import org.scalatest.FunSuite

import scala.reflect.io.Path

class DbManagerTest extends FunSuite {

  test("should generate new token and generation info if its not already saved") {
    val tmpDir = Paths.get(System.getProperty("java.io.tmpdir"))
    val metadata = new DbManager(tmpDir.toString).start(InetAddressAndPort.create("127.7.7.1", 8888))
    assert(metadata.generation == 1)
    assert(metadata.token != null)
  }

  test("should use existing token and generation info if its already saved") {
    val metadata = new DbManager("test").start(InetAddressAndPort.create("127.7.7.1", 8888))
    assert(metadata.generation == 1)
    assert(metadata.token != null)
  }

}
