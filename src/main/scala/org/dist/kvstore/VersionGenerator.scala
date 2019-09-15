package org.dist.kvstore

import java.util.concurrent.atomic.AtomicInteger

//needs to be singleton per server instance along with Gossiper
class VersionGenerator {
  private val version = new AtomicInteger(0)
  def incrementAndGetVersion: Int = version.incrementAndGet

  def currentVersion = version.get()

  def reset() = version.set(0)
}
