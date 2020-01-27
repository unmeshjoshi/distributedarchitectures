package org.dist.versionedkvstore

import java.util

import org.scalatest.FunSuite

class VersionedMapTest extends FunSuite {

  test("should get empty version for new key") {
    val map = new VersionedMap[String, String]()
    assert(0 == map.get("newKey").size())
  }

  test("should put versioned entry for new key") {
    val currentTime = System.currentTimeMillis()

    val client = new Client[String, String]()
    val version: Version = client.put("newKey", "newValue")

    assert("newValue" == client.get("newKey").value)

    val clock = new VectorClock()
    assert(clock.incremented(1, currentTime) == client.get("newKey").version)

    client.put("newKey", "anotherValue") //
    assert(clock.incremented(1, currentTime).incremented(1, currentTime) == client.get("newKey").version)
  }
}
