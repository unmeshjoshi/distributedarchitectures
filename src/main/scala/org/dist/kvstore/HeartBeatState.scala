package org.dist.kvstore

case class HeartBeatState(generation:Int, version:Int) {
  def updateVersion() = HeartBeatState(generation, VersionGenerator.getNextVersion)
}
