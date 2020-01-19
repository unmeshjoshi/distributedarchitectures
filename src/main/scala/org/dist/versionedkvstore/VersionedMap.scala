package org.dist.versionedkvstore

import java.util
import java.util.{ArrayList, List}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

class VersionedMap[K, V](map: ConcurrentMap[K, List[Versioned[V]]] = new ConcurrentHashMap[K, List[Versioned[V]]]()) {
  val metadataRefreshAttempts: Int = 2

  def put(key: K, value: Versioned[V]): Unit = {
    var items = map.get(key)
    // If we have no value, add the current value
    if (items == null) items = new util.ArrayList[Versioned[V]]
    // Check for existing versions - remember which items to
    // remove in case of success
    val itemsToRemove = new util.ArrayList[Versioned[V]](items.size)
    import scala.jdk.CollectionConverters._
    for (versioned <- items.asScala) {
      val occurred = value.getVersion.compare(versioned.getVersion)
      if (occurred eq Occurred.BEFORE) throw new ObsoleteVersionException("Obsolete version for key '" + key + "': " + value.getVersion)
      else if (occurred eq Occurred.AFTER) itemsToRemove.add(versioned)
    }
    items.removeAll(itemsToRemove)
    items.add(value)
    map.put(key, items)
  }

  def get(key: K): util.List[Versioned[V]] = {
    val results = map.get(key)
    if (results == null) new util.ArrayList[Versioned[V]](0)
    else new util.ArrayList[Versioned[V]](results)
  }

  def getVersions(key:K):util.List[Version] = getVersions(get(key))

  private def getVersions[V](versioneds: util.List[Versioned[V]]): util.List[Version] = {
    val versions = new util.ArrayList[Version]
    import scala.jdk.CollectionConverters._
    for (versioned <- versioneds.asScala) {
      versions.add(versioned.getVersion)
    }
    versions
  }
}
