package org.dist.kvstore.testapp

import java.io.File
import java.nio.file.{Path, Paths}

object Utils {

  def createDbDir(dbName: String) = {
    val tmpDir: Path = Paths.get(System.getProperty("java.io.tmpdir"))
    val node1Dir: String = tmpDir.toString + File.separator + dbName
    new File(node1Dir).mkdirs()
    node1Dir
  }
}
