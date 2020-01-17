package org.dist.versionedkvstore

import java.util

class InconsistentDataException(val message: String, var versions: util.List[Versioned[_]]) extends RuntimeException(message) {
  def getVersions = this.versions
}
