package org.dist.consensus.zab

import java.io.File

import org.dist.queue.TestUtils.random

object ZabTestUtils {
  def tempDir(): File = {
    val ioDir = System.getProperty("java.io.tmpdir")
    val f = new File(ioDir, "zab-" + random.nextInt(1000000))
    f.mkdirs()
    f.deleteOnExit()
    f
  }
}
