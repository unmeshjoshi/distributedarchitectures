package org.dist.akkagossip

import org.dist.patterns.common.InetAddressAndPort
import org.dist.queue.TestUtils
import org.scalatest.FunSuite

import scala.collection.immutable

class ClusterDaemonTest extends FunSuite {

  test("update gossipstate and vectorclock when new node joins") {
    val s1Address = InetAddressAndPort.create("localhost", 9999)
    val s1 = new ClusterDaemon(s1Address);
    s1.addJoiningMember(InetAddressAndPort.create("localhost", 9998))
    assert(s1.latestGossip.members.size == 2)
  }

  test("gossip state converges creates split brain if two seed nodes dont know about each other") {
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

    val networkIO = new DirectNetworkIO(Map(s1Address -> s1, s2Address -> s2, s3Address -> s3, s4Address -> s4, s5Address -> s5))

    s1.networkIO = networkIO
    s2.networkIO = networkIO
    s3.networkIO = networkIO
    s4.networkIO = networkIO
    s5.networkIO = networkIO

    //if s1 and s2 both act as seed nodes, but they don't know about each other, split brain can happen.
    //This puts extra restriction on seed nodes, that they all should know each other and completely join the cluster before they
    //can start receiving requests from other cluster nodes.

    s5.join(s1Address)

    TestUtils.waitUntilTrue(() => s1.membershipState.members.size == 2 && s1.membershipState.latestGossip.overview.seen.size == 2, "both nodes see the gossip")
    assert(s1.membershipState.isLeader(s5Address))

    s4.join(s1Address)

    TestUtils.waitUntilTrue(() => s1.membershipState.members.size == 3 && s1.membershipState.latestGossip.overview.seen.size == 3, "all three nodes see the gossip")
    assert(s4.membershipState.isLeader(s4Address))

    s3.join(s2Address)

    TestUtils.waitUntilTrue(() => s2.membershipState.members.size == 2 && s2.membershipState.latestGossip.overview.seen.size == 2, "all three nodes see the gossip")
    assert(s2.membershipState.isLeader(s2Address))

  }

  test("basic convergence") {
    val s1Address = InetAddressAndPort.create("localhost", 9999)
    val s1 = new ClusterDaemon(s1Address);

    val s2Address = InetAddressAndPort.create("localhost", 9995)
    val s2 = new ClusterDaemon(s2Address);

    val networkIO = new DirectNetworkIO(Map(s1Address -> s1, s2Address -> s2))

    s1.networkIO = networkIO
    s2.networkIO = networkIO

    println("Joining s1")
    s1.join(s1Address)

    println("Joining s2")
    s2.join(s1Address)

    TestUtils.waitUntilTrue(() => nodesConverge(s1, s2), "all nodes gossip converges and members marked as UP")
    assert(s1.membershipState.isLeader(s2Address))
  }

  test("node marks the node as unreachable if no response") {
    val s1Address = InetAddressAndPort.create("localhost", 9999)
    val s1 = new ClusterDaemon(s1Address);

    val s2Address = InetAddressAndPort.create("localhost", 9995)
    val s2 = new ClusterDaemon(s2Address);

    val networkIO = new DirectNetworkIO(Map(s1Address -> s1, s2Address -> s2))

    s1.networkIO = networkIO
    s2.networkIO = networkIO

    println("Joining s1")
    s1.join(s1Address)

    println("Joining s2")
    s2.join(s1Address)

    TestUtils.waitUntilTrue(() => nodesConverge(s1, s2), "all nodes gossip converges and members marked as UP")
    assert(s1.membershipState.isLeader(s2Address))
//
    Thread.sleep(2000)
    networkIO.disconnect(s2Address, s1Address)

    TestUtils.waitUntilTrue(()=> false == s1.isAliveInFailureDetector(s2Address), "node marked as down")
  }

  test("heartbeat state has all the members once nodes converge") {
    val s1Address = InetAddressAndPort.create("localhost", 9999)
    val s1 = new ClusterDaemon(s1Address);

    val s2Address = InetAddressAndPort.create("localhost", 9995)
    val s2 = new ClusterDaemon(s2Address);

    val networkIO = new DirectNetworkIO(Map(s1Address -> s1, s2Address -> s2))

    s1.networkIO = networkIO
    s2.networkIO = networkIO

    println("Joining s1")
    s1.join(s1Address)

    println("Joining s2")
    s2.join(s1Address)

    TestUtils.waitUntilTrue(() => nodesConverge(s1, s2), "all nodes gossip converges and members marked as UP")
    assert(s1.membershipState.isLeader(s2Address))

  }

  test("basic convergence 3 nodes") {
    val s1Address = InetAddressAndPort.create("localhost", 9999)
    val s1 = new ClusterDaemon(s1Address);

    val s2Address = InetAddressAndPort.create("localhost", 9995)
    val s2 = new ClusterDaemon(s2Address);

    val s3Address = InetAddressAndPort.create("localhost", 9996)
    val s3 = new ClusterDaemon(s3Address);

    val networkIO = new DirectNetworkIO(Map(s1Address -> s1, s2Address -> s2, s3Address -> s3))

    s1.networkIO = networkIO
    s2.networkIO = networkIO
    s3.networkIO = networkIO

    println("Joining s1")
    s1.join(s1Address)

    println("Joining s2")
    s2.join(s1Address)

    println("Joining s3")
    s3.join(s1Address)

    TestUtils.waitUntilTrue(() => nodesConverge(s1, s2, s3), "all nodes gossip converges and members marked as UP")
    assert(s1.membershipState.isLeader(s2Address))

    TestUtils.waitUntilTrue(() => s3.isMonitoring(s2Address), "s5 got a few heartbeats from s2")
    TestUtils.waitUntilTrue(() => s1.isMonitoring(s2Address), "s1 got a few heartbeats from s2")

    networkIO.disconnect(s2Address, s3Address)

    TestUtils.waitUntilTrue(() => {
      s3.isUnreachableInFailureDetector(s2Address)
    }, "S3  marks s2 as unrachable")

    //with gossip, S2 will be marked unreachable in all the nodes.
    TestUtils.waitUntilTrue(() => {
      (!s3.membershipState.latestGossip.overview.reachability.isReachable(s2Address) &&
        !s1.membershipState.latestGossip.overview.reachability.isReachable(s2Address))
    }, "All nodes mark s2 as unrachable")

    println("Reachability in s1")
    println(s1.membershipState.latestGossip.overview.reachability.records)
    assert(s1.membershipState.isLeader(s3Address))
  }

  test("basic convergence 4 nodes") {
    val s1Address = InetAddressAndPort.create("localhost", 9999)
    val s1 = new ClusterDaemon(s1Address);

    val s2Address = InetAddressAndPort.create("localhost", 9995)
    val s2 = new ClusterDaemon(s2Address);

    val s3Address = InetAddressAndPort.create("localhost", 9996)
    val s3 = new ClusterDaemon(s3Address);

    val s4Address = InetAddressAndPort.create("localhost", 9997)
    val s4 = new ClusterDaemon(s4Address);

    val networkIO = new DirectNetworkIO(Map(s1Address -> s1, s2Address -> s2, s3Address -> s3, s4Address -> s4))

    s1.networkIO = networkIO
    s2.networkIO = networkIO
    s3.networkIO = networkIO
    s4.networkIO = networkIO

    println("Joining s1")
    s1.join(s1Address)

    println("Joining s2")
    s2.join(s1Address)

    println("Joining s3")
    s3.join(s1Address)

    println("Joining s4")
    s4.join(s1Address)

    TestUtils.waitUntilTrue(() => nodesConverge(s1, s2, s3, s4), "all nodes gossip converges and members marked as UP")
    assert(s1.membershipState.isLeader(s2Address))
  }

  test("Memberlist union test") {
    val joiningMembers = immutable.SortedSet(Member(InetAddressAndPort.create("localhost", 9999)),
      Member(InetAddressAndPort.create("localhost", 9998)))

    val upMembers = immutable.SortedSet(Member(InetAddressAndPort.create("localhost", 9999)).copyUp(1),
      Member(InetAddressAndPort.create("localhost", 9998)).copyUp(2))

    val result = upMembers.union(joiningMembers.diff(upMembers))

    assert(result == upMembers)
  }


  test("basic convergence 5 nodes") {
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

    val networkIO = new DirectNetworkIO(Map(s1Address -> s1, s2Address -> s2, s3Address -> s3, s4Address -> s4, s5Address -> s5))

    s1.networkIO = networkIO
    s2.networkIO = networkIO
    s3.networkIO = networkIO
    s4.networkIO = networkIO
    s5.networkIO = networkIO

    println("Joining s1")
    s1.join(s1Address)

    println("Joining s2")
    s2.join(s1Address)

    println("Joining s3")
    s3.join(s1Address)

    println("Joining s4")
    s4.join(s1Address)

    TestUtils.waitUntilTrue(() => nodesConverge(s1, s2, s3, s4), "all nodes gossip converges and members marked as UP")

    println("Joining s5")
    s5.join(s1Address)

    TestUtils.waitUntilTrue(() => nodesConverge(s1, s2, s3, s4, s5), "all nodes gossip converges and members marked as UP")
    assert(s1.membershipState.isLeader(s2Address))
  }

  test("partial network failure marks members as unreachable") {
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

    val networkIO = new DirectNetworkIO(Map(s1Address -> s1, s2Address -> s2, s3Address -> s3, s4Address -> s4, s5Address -> s5))

    s1.networkIO = networkIO
    s2.networkIO = networkIO
    s3.networkIO = networkIO
    s4.networkIO = networkIO
    s5.networkIO = networkIO

    println("Joining s1")
    s1.join(s1Address)

    println("Joining s2")
    s2.join(s1Address)

    println("Joining s3")
    s3.join(s1Address)

    println("Joining s4")
    s4.join(s1Address)

    TestUtils.waitUntilTrue(() => nodesConverge(s1, s2, s3, s4), "all nodes gossip converges and members marked as UP")

    println("Joining s5")
    s5.join(s1Address)

    TestUtils.waitUntilTrue(() => nodesConverge(s1, s2, s3, s4, s5), "all nodes gossip converges and members marked as UP")
    assert(s1.membershipState.isLeader(s2Address))

    TestUtils.waitUntilTrue(() => s5.isMonitoring(s2Address), "s5 got a few heartbeats from s2")

    networkIO.disconnect(s2Address, s5Address)

    TestUtils.waitUntilTrue(() => {
      s5.isUnreachableInFailureDetector(s2Address)
    }, "S5  marks s2 as unrachable")

    TestUtils.waitUntilTrue(() => {
      !s1.membershipState.latestGossip.overview.reachability.isReachable(s2Address)},
      "S1 still sees s2 as reachable")

    assert(s1.membershipState.isLeader(s3Address))
  }



  def createClusterNodes(n: Int): List[ClusterDaemon] = {
    val basePort = 8000
    List.range(1, n+1).map(index => new ClusterDaemon(InetAddressAndPort.create("localhost",  basePort + index)))
  }


  test("partial network failure - split brain resolution") {
    val nodes = createClusterNodes(7)

    val addressToNodeMap = nodes.map(n => (n.selfUniqueAddress, n)).toMap
    val networkIO = new DirectNetworkIO(addressToNodeMap)
    nodes.foreach(n => n.networkIO = networkIO)

    val leader = nodes.sorted(ClusterDaemon.clusterNodeOrdering).head
    nodes.foreach(n => n.join(leader.selfUniqueAddress))

    TestUtils.waitUntilTrue(() => nodesConverge(nodes:_*), "all nodes gossip converges and members marked as UP")

    val splitAt = 3
    val firstPartition = nodes.slice(0, splitAt)
    val secondPartition = nodes.slice(splitAt, nodes.size)

    TestUtils.waitUntilTrue(() => firstPartition.forall(n => {
      secondPartition.exists(s => n.isMonitoring(s.selfUniqueAddress))
    }), "waiting till few heartbeats are transferred")

    TestUtils.waitUntilTrue(() => secondPartition.forall(n => {
      firstPartition.exists(s => n.isMonitoring(s.selfUniqueAddress))
    }), "waiting till few heartbeats are transferred")

    networkIO.disconnect(firstPartition.map(n => n.selfUniqueAddress), secondPartition.map(n => n.selfUniqueAddress))
    networkIO.disconnect(secondPartition.map(n => n.selfUniqueAddress), firstPartition.map(n => n.selfUniqueAddress))

    TestUtils.waitUntilTrue(() => {
      notReachable(firstPartition, secondPartition)
    },
      "0,1,2 mark 3,4,6,7 as unreachable")

    TestUtils.waitUntilTrue(() => {
      notReachable(secondPartition, firstPartition)
    },
      "3,4,6,7 mark 0,1,2 as unreachable")

    assert(firstPartition.forall(n => {
      println(s"Leader in ${n.selfUniqueAddress} is ${n.membershipState.leader}")
      println(n.membershipState.latestGossip.overview.reachability)
      n.membershipState.isLeader(firstPartition.sorted.head.selfUniqueAddress)
    }))
    assert(secondPartition.forall(n => {
      println(s"Leader in ${n.selfUniqueAddress} is ${n.membershipState.leader}")
      println(n.membershipState.latestGossip.overview.reachability)
      n.membershipState.isLeader(secondPartition.sorted.head.selfUniqueAddress)
    }))

    //TODO: different leaders in different partitions
  }


  private def notReachable(nodes:List[ClusterDaemon], disconnectedNodes:List[ClusterDaemon]):Boolean = {
    nodes.forall(n => {
      disconnectedNodes.forall(dn => {
        !n.membershipState.latestGossip.overview.reachability.isReachable(dn.selfUniqueAddress)
      })
    })
  }

  test("Leadership changes to lower ip address nodes but upNumbers remain as is the new nodes join the network") {
    val s1Address = InetAddressAndPort.create("localhost", 9999)
    val s1 = new ClusterDaemon(s1Address);

    val s2Address = InetAddressAndPort.create("localhost", 9995)
    val s2 = new ClusterDaemon(s2Address);

    val s3Address = InetAddressAndPort.create("localhost", 9996)
    val s3 = new ClusterDaemon(s3Address);

    val s5Address = InetAddressAndPort.create("localhost", 9998)
    val s5 = new ClusterDaemon(s5Address);

    val s4Address = InetAddressAndPort.create("localhost", 9997)
    val s4 = new ClusterDaemon(s4Address);

    val networkIO = new DirectNetworkIO(Map(s1Address -> s1, s2Address -> s2, s3Address -> s3, s4Address -> s4, s5Address -> s5))

    s1.networkIO = networkIO
    s2.networkIO = networkIO
    s3.networkIO = networkIO
    s4.networkIO = networkIO
    s5.networkIO = networkIO


    //only s1 and s5 join the cluster. S5 is elected is the leader. UpNumber for s5 is 1
    s1.join(s1Address)
    s5.join(s1Address)

    TestUtils.waitUntilTrue(() => nodesConverge(s1, s5), "all nodes see the gossip")
    assert(s1.membershipState.isLeader(s5Address))
    assert(s1.oldestMember().upNumber == 1)
    assert(s1.oldestMember().address == s5Address)

    s1.membershipState.members.foreach(m => println(m))

    //s4, s3, s2 join the cluster. s2 is now the leader. But upNumber remains same for all the existing nodes.
    s4.join(s1Address)
    s3.join(s1Address)
    s2.join(s1Address)

    TestUtils.waitUntilTrue(() => nodesConverge(s1, s2,s5,  s3, s4), "all five nodes see the gossip")
    assert(s2.membershipState.isLeader(s2Address))
    assert(s2.oldestMember().upNumber == 1)
    assert(s2.oldestMember().address == s5Address)


    println(s1)
  }


  import scala.jdk.CollectionConverters._
  private def nodesConverge(nodes: ClusterDaemon*) = {
    nodes.forall(d => d.allMembersUp(nodes.size)) &&
      nodes.forall(d => d.heartbeat.state.ring.nodes.size == nodes.size)
  }
}
