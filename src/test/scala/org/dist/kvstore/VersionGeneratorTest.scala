package org.dist.kvstore

import org.scalatest.FunSuite

class VersionGeneratorTest extends FunSuite {
  test("should increment version on every call") {
    assert(VersionGenerator.getNextVersion < VersionGenerator.getNextVersion)
  }
}
