package org.dist.consensus.zab

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.TestUtils
import org.dist.util.Networks
import org.scalatest.FunSuite

class QuorumPeerTest extends FunSuite {

  test("should elect leader and commit values after getting ACKs from majority of followers") {
    val address = new Networks().ipv4Address
    val peerAddr1 = InetAddressAndPort(address, 9998)
    val peerAddr2 = InetAddressAndPort(address, 9999)
    val peerAddr3 = InetAddressAndPort(address, 9997)

    val addrForClients1 = InetAddressAndPort(address, 9080)
    val addrForClients2 = InetAddressAndPort(address, 9081)
    val addrForClients3 = InetAddressAndPort(address, 9082)

    val serverList = List(QuorumServer(1, peerAddr1, addrForClients1), QuorumServer(2, peerAddr2, addrForClients2), QuorumServer(3, peerAddr3, addrForClients3))

    val config1 = QuorumPeerConfig(1, peerAddr1, addrForClients1, serverList, TestUtils.tempDir().getAbsolutePath)
    val peer1 = new QuorumPeer(config1, new QuorumConnectionManager(config1))

    val config2 = QuorumPeerConfig(2, peerAddr2, addrForClients2, serverList, TestUtils.tempDir().getAbsolutePath)
    val peer2 = new QuorumPeer(config2, new QuorumConnectionManager(config2))

    val config3 = QuorumPeerConfig(3, peerAddr3, addrForClients3, serverList, TestUtils.tempDir().getAbsolutePath)
    val peer3 = new QuorumPeer(config3, new QuorumConnectionManager(config3))

    peer1.start()
    peer2.start()
    peer3.start()

    TestUtils.waitUntilTrue(() ⇒ {
      peer3.state == ServerState.LEADING && peer3.leader != null && peer3.leader.newLeaderProposal.ackCount >= 2
    }, "Waiting for leader receiving ACKs", 10000)

    //peer3 will be leader because it has highest id, and there is no history
    TestUtils.waitUntilTrue(() ⇒ {
      peer3.leader.cnxn.serverSocket != null && peer3.leader.cnxn.serverSocket.isBound
    }, "Waiting for server to accept client connections")



    println("Sending request to quorum")
    new Client(config3.serverAddress).setData("/greetPath", "Hello World!")

    //assert that majority has the data once client gets response. At this point its committed
    assert(leaderHasDataFor(peer3, "/greetPath") &&
      followerHasDataFor(peer2, "/greetPath") || followerHasDataFor(peer1, "/greetPath"))


    //assert all the peers get the data eventually
    TestUtils.waitUntilTrue(()⇒ {
      leaderHasDataFor(peer3, "/greetPath") &&
      followerHasDataFor(peer2, "/greetPath") &&
      followerHasDataFor(peer1, "/greetPath")
    }, "Waiting till the value reaches all peers")
  }

  private def leaderHasDataFor(peer: QuorumPeer, key:String) = {
    peer.leader.zk.dataTree.nodes.containsKey(key)
  }

  private def followerHasDataFor(peer: QuorumPeer, key:String) = {
    info(s"Checking for ${peer.config.serverId}" )
    peer.follower != null && peer.follower.zk.dataTree.nodes.containsKey(key)
  }
}
