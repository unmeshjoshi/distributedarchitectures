package org.dist.kvstore

case class HeartBeatState(generation:Int, version:Int) {
  def updateVersion(version:Int) = HeartBeatState(generation, version)
}
