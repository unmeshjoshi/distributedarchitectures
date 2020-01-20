package org.dist.versionedkvstore

import java.util

import org.scalatest.FunSuite

class VersionedMapTest extends FunSuite {

  test("should get empty version for new key") {
    val map = new VersionedMap[String, String]()
    assert(0 == map.get("newKey").size())
  }

  test("should put versioned entry for new key") {
    val client = new Client[String, String]()
    val version: Version = client.put("newKey", "newValue")

    assert("newValue" == client.get("newKey").value)
    assert(new VectorClock().incremented(1, System.currentTimeMillis()) == client.get("newKey").version)

    client.put("newKey", "anotherValue")
    assert(new VectorClock().incremented(1, System.currentTimeMillis()).incremented(1, System.currentTimeMillis()) == client.get("newKey").version)
  }
}
