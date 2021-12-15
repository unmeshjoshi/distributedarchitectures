package org.dist.akkagossip

import org.dist.patterns.common.InetAddressAndPort
import org.scalatest.FunSuite

class ClusterDaemonTest extends FunSuite {

  test("gossip converges membership state") {
    val s1Address = InetAddressAndPort.create("localhost", 9999)
    val s1 = new ClusterDaemon(s1Address);
    val s2Address = InetAddressAndPort.create("localhost", 9995)
    val s2 = new ClusterDaemon(s2Address);
    val s3Address = InetAddressAndPort.create("localhost", 9996)
    val s3 = new ClusterDaemon(s3Address);
    val s4Address = InetAddressAndPort.create("localhost", 9997)
    val s4 = new ClusterDaemon(s4Address);
    val s5Address = InetAddressAndPort.create("localhost", 9998)
    val s5 = new ClusterDaemon(s5Address);

    val networkIO = new DirectNetworkIO();
    networkIO.connections = Map(s1Address -> s1, s2Address -> s2, s3Address -> s3, s4Address -> s4, s5Address -> s5)

    s1.networkIO = networkIO
    s2.networkIO = networkIO
    s3.networkIO = networkIO
    s4.networkIO = networkIO
    s5.networkIO = networkIO

    s1.join(s1Address)
    s2.join(s1Address)
    s3.join(s1Address)
    s4.join(s1Address)
    s5.join(s1Address)
    println("Done")

    Thread.sleep(10000)
  }

}
