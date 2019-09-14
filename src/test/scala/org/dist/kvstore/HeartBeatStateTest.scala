package org.dist.kvstore

import org.scalatest.FunSuite

class HeartBeatStateTest extends FunSuite {

  test("should increment version on update") {
    val state = HeartBeatState(1, VersionGenerator.getNextVersion)
    val newState = state.updateVersion()
    assert(newState.version > state.version)
  }

}
