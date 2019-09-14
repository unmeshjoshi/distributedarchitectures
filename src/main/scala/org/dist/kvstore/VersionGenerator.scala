package org.dist.kvstore

import java.util.concurrent.atomic.AtomicInteger

object VersionGenerator {
  private val version = new AtomicInteger(0)
  def getNextVersion: Int = version.incrementAndGet

  def currentVersion = version.get()

  def reset() = version.set(0)
}
