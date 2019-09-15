package org.dist.kvstore

import org.scalatest.FunSuite

class VersionGeneratorTest extends FunSuite {
  test("should increment version on every call") {
    val versionGenerator = new VersionGenerator
    assert(versionGenerator.incrementAndGetVersion < versionGenerator.incrementAndGetVersion)
  }
}
