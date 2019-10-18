package org.dist.consensus.zab

import org.scalatest.FunSuite

class QuorumPeerTest extends FunSuite {

  val quorumConnectionManager: QuorumConnectionManager = ???

  test("should be in looking state till it either becomes leader or follower") {
    val peer = new QuorumPeer(QuorumPeerConfig(1, List()), quorumConnectionManager)

  }
}
