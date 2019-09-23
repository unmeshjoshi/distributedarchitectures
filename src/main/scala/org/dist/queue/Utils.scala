package org.dist.queue

import java.io.File

object Utils {

  def swallow(log: (Object, Throwable) => Unit, action: => Unit) = {
    try {
      action
    } catch {
      case e: Throwable => log(e.getMessage(), e)
    }
  }
  def rm(file: File) {
    if (file == null) {
      return
    } else if (file.isDirectory) {
      val files = file.listFiles()
      if (files != null) {
        for (f <- files)
          rm(f)
      }
      file.delete()
    } else {
      file.delete()
    }
  }
}
