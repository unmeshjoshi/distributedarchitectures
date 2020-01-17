package org.dist.versionedkvstore

import java.util
import java.util.{ArrayList, List}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

class VersionedMap[K, V](map: ConcurrentMap[K, List[Versioned[V]]] = new ConcurrentHashMap[K, List[Versioned[V]]]()) {
  val metadataRefreshAttempts: Int = 2

  private def getVersionWithResolution(key: K) = {
    val allValues = getAllVersions(key)
    val versions = getVersions(allValues)
    if (versions.isEmpty) null
    else if (versions.size == 1) versions.get(0)
    else {
      val versioned = get(key)
      if (versioned == null) null
      else versioned.getVersion
    }
  }

  protected def getItemOrThrow(key: K, defaultValue: Versioned[V], items: util.List[Versioned[V]]): Versioned[V] = if (items.size == 0) defaultValue
  else if (items.size == 1) items.get(0)
  else throw new InconsistentDataException("Unresolved versions returned from get(" + key + ") = " + items, items.asInstanceOf[java.util.List[Versioned[_]]])

  def get(key: K): Versioned[V] = get(key, null)

  def get(key: K, defaultValue: Versioned[V]): Versioned[V] = {
    for (attempts <- 0 until this.metadataRefreshAttempts) {
      try {
        val items = getAllVersions(key)
        return getItemOrThrow(key, defaultValue, items)
      } catch {
        case _ ⇒ throw new RuntimeException()
//        case e: InvalidMetadataException ⇒
//          logger.info("Received invalid metadata exception during get [  " + e.getMessage + " ] on store '" + storeName + "'. Rebootstrapping")
//          bootStrap()
      }
    }
    throw new RuntimeException(this.metadataRefreshAttempts + " metadata refresh attempts failed.")
  }

  def getVersionForPut(key: K) = {
    var version = getVersionWithResolution(key)
    if (version == null) version = new VectorClock
    version
  }


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

  def getAllVersions(key: K): util.List[Versioned[V]] = {
    val results = map.get(key)
    if (results == null) new util.ArrayList[Versioned[V]](0)
    else new util.ArrayList[Versioned[V]](results)
  }

  def getVersions[V](versioneds: util.List[Versioned[V]]): util.List[Version] = {
    val versions = new util.ArrayList[Version]
    import scala.jdk.CollectionConverters._
    for (versioned <- versioneds.asScala) {
      versions.add(versioned.getVersion)
    }
    versions
  }
}
