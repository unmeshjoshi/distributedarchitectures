package org.dist.kvstore

import java.io.File
import java.nio.file.Paths

import org.scalatest.FunSuite

class DbManagerTest extends FunSuite {

  test("should generate new token and generation info if its not already saved") {
    val tmpDir = Paths.get(System.getProperty("java.io.tmpdir"))
    try {
      val metadata = new DbManager(tmpDir.toString).start(InetAddressAndPort.create("127.7.7.1", 8888))
      assert(metadata.generation == 1)
      assert(metadata.token != null)
    } finally {
      new File(s"${tmpDir}${File.pathSeparator}system.db").delete()
    }
  }

  test("should use existing token and increment generation info if its already saved") {
    val tmpDir = Paths.get(System.getProperty("java.io.tmpdir"))

    try {

      //start to create a file
      new DbManager(tmpDir.toString).start(InetAddressAndPort.create("127.7.7.1", 8888))

      val metadata = new DbManager(tmpDir.toString).start(InetAddressAndPort.create("127.7.7.1", 8888))
      assert(metadata.generation == 2)
      assert(metadata.token != null)
    } finally {
      new File(s"${tmpDir}${File.pathSeparator}system.db").delete()
    }
  }

}
