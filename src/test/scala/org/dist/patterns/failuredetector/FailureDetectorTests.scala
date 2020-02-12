package org.dist.patterns.failuredetector

import org.dist.kvstore.InetAddressAndPort
import org.dist.patterns.replicatedlog.heartbeat.Peer
import org.dist.queue.TestUtils
import org.dist.util.Networks
import org.scalatest.FunSuite

class FailureDetectorTests extends FunSuite {

  test("TimeoutBasedFailure Detector should detect liveness with heatbeat") {
    val localHost = new Networks().hostname()
    val senderIp = InetAddressAndPort.create(localHost, TestUtils.choosePort())
    val receiverIp = InetAddressAndPort.create(localHost, TestUtils.choosePort())

    val sender = new Sender(1, List(Peer(2, receiverIp)))
    val receiver = new Receiver(receiverIp, List(Peer(1, senderIp)), new SimpleFailureDetector[Int]())

    sender.start()
    receiver.start()

    TestUtils.waitUntilTrue(() ⇒ {
      receiver.failureDetector.isAlive(1)
    }, "Waiting for server 1 to be detected as alive")

    sender.stop()

    TestUtils.waitUntilTrue(() ⇒ {
      receiver.failureDetector.isAlive(1) == false
    }, "Waiting for server 1 to be detected as crashed")

    receiver.stop
  }


  test("PhiChiFailure Detector should detect liveness with heatbeat") {
    val localHost = new Networks().hostname()
    val senderIp = InetAddressAndPort.create(localHost, TestUtils.choosePort())
    val receiverIp = InetAddressAndPort.create(localHost, TestUtils.choosePort())

    val sender = new Sender(1, List(Peer(2, receiverIp)))
    val receiver = new Receiver(receiverIp, List(Peer(1, senderIp)), new PhiChiAccrualFailureDetector[Int]())

    sender.start()
    receiver.start()

    TestUtils.waitUntilTrue(() ⇒ {
      receiver.failureDetector.isAlive(1)
    }, "Waiting for server 1 to be detected as alive")

    sender.stop()

    TestUtils.waitUntilTrue(() ⇒ {
      receiver.failureDetector.isAlive(1) == false
    }, "Waiting for server 1 to be detected as crashed", waitTimeMs = 15000)

    receiver.stop
  }
}
