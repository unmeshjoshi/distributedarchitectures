package org.dist.versionedkvstore

import org.scalatest.FunSuite

class VersionedMapTest extends FunSuite {

  test("should get empty version for new key") {
    val map = new VersionedMap[String, String]()
    assert(0 == map.getAllVersions("newKey").size())
  }


  test("should put versioned entry for new key") {
    val map = new VersionedMap[String, String]()
    val version: Version = map.getVersionForPut("newKey")
    val versioned = Versioned("newValue", version)
    val versionedClock = versioned.getVersion.asInstanceOf[VectorClock]

    //following happens for master node
//    val id = node.getId
    val id = 1

    val versionedCopy = new Versioned[String](versioned.getValue, versionedClock.incremented(id, System.currentTimeMillis))
    map.put("newKey", versionedCopy)

    val versions = map.getAllVersions("newKey")
    assert(1 == versions.size())
  }
}
