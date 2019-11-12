package org.dist.kvstore

import java.util.concurrent.atomic.AtomicInteger

case class HeartBeatState(generation:Int, version:Int, heartBeat:AtomicInteger = new AtomicInteger(0)) {
  def updateVersion(version:Int) = {
    heartBeat.incrementAndGet()
    HeartBeatState(generation, version, heartBeat)
  }

  def updateHeartBeat(version:Int) = {
    heartBeat.incrementAndGet()
    HeartBeatState(generation, version, heartBeat)
  }
}
