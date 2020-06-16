package org.dist.versionedkvstore

import org.scalatest.FunSuite
import scala.jdk.CollectionConverters._
import java.util

class VersionedMapTest extends FunSuite {

  test("should get empty version for new key") {
    val map = new VersionedMap[String, String]()
    assert(0 == map.get("newKey").size())
  }

  test("should put versioned entry for new key") {
    val currentTime = System.currentTimeMillis()

    val node1 = new Node[String, String](1, List(1, 2, 3).asJava)
    val node2 = new Node[String, String](2, List(4, 5, 6).asJava)
    val node3 = new Node[String, String](3, List(7, 8, 9).asJava)

    val client = new Client[String, String](List(node1, node2, node3),new FailureDetector[String, String]())
    val version: Version = client.put("k1", "v1")

    assert("v1" == client.get("k1").value)

    val newClock = new VectorClock()
    val clock1 = newClock.incremented(1, currentTime)
    assert(clock1 == client.get("k1").version)

    client.put("k1", "v2") //
    val clock = clock1.incremented(1, currentTime)
    assert(clock == client.get("k1").version)
  }


  test("should resolve versioned entry based on vectorclock") {
    val currentTime = System.currentTimeMillis()

    val node1 = new Node[String, String](1, List(1, 2, 3).asJava)
    val node2 = new Node[String, String](2, List(4, 5, 6).asJava)
    val node3 = new Node[String, String](3, List(7, 8, 9).asJava)


    val nodes = List(node1, node2, node3)
    val nodeMap = new util.HashMap[Int, Node[String, String]]
    nodes.foreach(node => nodeMap.put(node.id, node))

    val client = new Client[String, String](nodes,new FailureDetector[String, String]())
    val version: Version = client.put("k1", "v1")

    assert("v1" == client.get("k1").value)

    val newClock = new VectorClock()
    val clock1 = newClock.incremented(1, currentTime)
    assert(clock1 == client.get("k1").version)

    val updateVersion = client.put("k1", "v2", true) //
    assert(updateVersion == new VectorClock().incremented(1, currentTime).incremented(2, currentTime))


    val nodeValues = client.getNodeValues("k1")
    val repairs = new ReadRepairer[String, String]().getRepairs(nodeValues)
    repairs.asScala.foreach(repair => {
      val id = repair.getNodeId
      nodeMap.get(id).put(repair.getKey, repair.getVersioned)
    })


    val updateVersion2 = client.put("k1", "v3") //
    assert(updateVersion2 == new VectorClock().incremented(1, currentTime).incremented(1, currentTime).incremented(2, currentTime))

    val clock = clock1.incremented(1, currentTime).incremented(2, currentTime)
    assert(clock == client.get("k1").version)
  }
}
