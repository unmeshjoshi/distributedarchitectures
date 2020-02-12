package org.dist.patterns.replicatedlog.heartbeat

import org.dist.queue.TestUtils
import org.scalatest.FunSuite

class HeartBeatSchedulerTest extends FunSuite {

  test("Should invoke action when scheduler triggers") {
    var actionCalled = false
    val action = () ⇒ {actionCalled = true}
    val heartBeat = new HeartBeatScheduler(action)

    heartBeat.start()

    TestUtils.waitUntilTrue(()⇒{actionCalled}, "Waiting for action to be called")
    heartBeat.cancel()
  }



  test("Should be able to cancel scheduled task") {
    var actionCalled = false
    val action = () ⇒ {actionCalled = true}
    val heartBeat = new HeartBeatScheduler(action)

    heartBeat.start()

    TestUtils.waitUntilTrue(()⇒{actionCalled}, "Waiting for action to be called")

    actionCalled = false
    heartBeat.cancel()
    try {
    TestUtils.waitUntilTrue(() ⇒ {
      actionCalled
    }, "waiting for action to be called", 500)
    } catch {
      case e:RuntimeException ⇒
    }
  }
}
