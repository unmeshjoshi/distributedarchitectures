package org.dist.versionedkvstore

import org.scalatest.FunSuite
import scala.jdk.CollectionConverters._

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
    val version: Version = client.put("newKey", "newValue")

    assert("newValue" == client.get("newKey").value)

    val newClock = new VectorClock()
    val clock1 = newClock.incremented(1, currentTime)
    assert(clock1 == client.get("newKey").version)

    client.put("newKey", "anotherValue") //
    val clock = clock1.incremented(1, currentTime)
    assert(clock == client.get("newKey").version)
  }
}
