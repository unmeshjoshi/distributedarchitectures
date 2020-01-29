package org.dist.patterns.replicatedlog

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.TestUtils
import org.dist.util.Networks
import org.scalatest.FunSuite

class ServerTest extends FunSuite {

  test("should elect leader") {
    val address = new Networks().ipv4Address
    val peerAddr1 = InetAddressAndPort(address, 9998)
    val peerAddr2 = InetAddressAndPort(address, 9999)
    val peerAddr3 = InetAddressAndPort(address, 9997)


    val serverList = List(ServerConfig(1, peerAddr1), ServerConfig(2, peerAddr2), ServerConfig(3, peerAddr3))

    val config1 = Config(1, peerAddr1, serverList, TestUtils.tempDir())
    val peer1 = new Server(config1)

    val config2 = Config(2, peerAddr2, serverList, TestUtils.tempDir())
    val peer2 = new Server(config2)

    val config3 = Config(3, peerAddr3, serverList, TestUtils.tempDir())
    val peer3 = new Server(config3)

    peer1.startListening()
    peer2.startListening()
    peer3.startListening()

    peer1.start()
    peer2.start()
    peer3.start()

    TestUtils.waitUntilTrue(()⇒ {
      peer3.state == ServerState.LEADING
    }, "Waiting for leader to be selected")

  }
}
