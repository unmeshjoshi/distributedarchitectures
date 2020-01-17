package org.dist.versionedkvstore

/**
 * An exception that indicates an attempt by the user to overwrite a newer value
 * for a given key with an older value for the same key. This is a
 * application-level error, and indicates the application has attempted to write
 * stale data.
 *
 *
 */
class ObsoleteVersionException(message: String, cause: Exception)  extends RuntimeException(message, cause) {
  def this(message: String ) {
    this(message, null)
  }
}

