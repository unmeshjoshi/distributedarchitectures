package org.dist.consensus.zab

import org.dist.kvstore.InetAddressAndPort
import org.dist.util.Networks
import org.scalatest.FunSuite

class QuorumPeerTest extends FunSuite {

  test("should be in looking state till it either becomes leader or follower") {
    val quorumConnectionManager: QuorumConnectionManager = new QuorumConnectionManager()

    val address = new Networks().ipv4Address
    val peerAddr1 = InetAddressAndPort(address, 8080)
    val peerAddr2 = InetAddressAndPort(address, 8081)
    val peerAddr3 = InetAddressAndPort(address, 8082)

    val serverAddr1 = InetAddressAndPort(address, 9080)
    val serverAddr2 = InetAddressAndPort(address, 9081)
    val serverAddr3 = InetAddressAndPort(address, 9082)

    val serverList = List(QuorumServer(peerAddr1, serverAddr1), QuorumServer(peerAddr2, serverAddr2))
    
    val peer1 = new QuorumPeer(QuorumPeerConfig(1, peerAddr1,serverAddr1, serverList ), quorumConnectionManager)
    val peer2 = new QuorumPeer(QuorumPeerConfig(2, peerAddr2,serverAddr1, serverList), quorumConnectionManager)
//    val peer3 = new QuorumPeer(QuorumPeerConfig(3, peerAddr3,serverAddr1, serverList), quorumConnectionManager)

    peer1.start()
    peer2.start()
//    peer3.start()

    Thread.sleep(10000)
  }
}
