package org.dist.kvstore

import java.io.File
import java.nio.file.Paths

object TestUtils {

  def tmpDir = {
    Paths.get(System.getProperty("java.io.tmpdir"))
  }

  def deleteTable(table:String) = {
    new File(s"${TestUtils.tmpDir}${System.getProperty("file.separator") }${table}.db").delete()
  }
}
