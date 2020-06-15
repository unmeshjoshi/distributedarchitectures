package org.dist.versionedkvstore

import java.util

import org.dist.queue.common.Logging
import scala.jdk.CollectionConverters._

class FailureDetector[K, V] {
  def isAvailable(node: Node[K, V]): Unit = {
    return true
  }
}

class Client[K, V](nodes: List[Node[K, V]], failureDetector: FailureDetector[K, V]) extends Logging {
  val metadataRefreshAttempts: Int = 2

  def put(key: K, value: V, failMaster: Boolean = false): Version = {

    val server = if (failMaster) nodes(1) else nodes(0) //assume node1 is always the master and node2 is replica
    val version = getVersionForPut(key)
    val versioned = Versioned.value(value, version)
    val versionedClock = versioned.getVersion.asInstanceOf[VectorClock]

    val versionedCopy = new Versioned[V](versioned.getValue, versionedClock.incremented(server.id, System.currentTimeMillis()))
    server.put(key, versionedCopy)

    val replica = nodes(1);
    if (!failMaster) { //if we failed master, the replica is already written to.
      replica.put(key, versionedCopy);
      versionedCopy.getVersion
    }
    versionedCopy.getVersion
  }

  import scala.jdk.CollectionConverters._

  protected def getItemOrThrow(key: K, items: util.List[Versioned[V]]): Versioned[V] = {
    if (items.size == 0) null //default Value
    else if (items.size == 1) items.get(0)
    else throw new InconsistentDataException("Unresolved versions returned from get(" + key + ") = " + items, items.asInstanceOf[java.util.List[Versioned[_]]])
  }

  protected var storeName: String = "defaultStore"

  def get(key: K, failMaster:Boolean = false): Versioned[V] = {
    for (attempts <- 0 until this.metadataRefreshAttempts) {
      try {
        //TODO: Read from multiple nodes and do read repair
        val server = if (!failMaster) nodes(0) else nodes(1)
        val items: util.List[Versioned[V]] = server.get(key)
        val resolvedItems = new VectorClockInconsistencyResolver[V]().resolveConflicts(items)
        return getItemOrThrow(key, resolvedItems)
      } catch {
        case e: Exception ⇒
          info("Received invalid metadata exception during get [  " + e.getMessage + " ] on store '" + storeName + "'. Rebootstrapping")
          bootStrap()
      }
    }
    throw new RuntimeException(this.metadataRefreshAttempts + " metadata refresh attempts failed.")
  }

  def bootStrap(): Unit = {

  }

  def getVersionWithResolution(key: K) = {
    val versions = nodes.flatMap(n ⇒ n.getVersions(key).asScala)
    if (versions.isEmpty) null
    else if (versions.size == 1) versions(0)
    else {
      val versioned = get(key)
      if (versioned == null) null
      else versioned.getVersion
    }
  }

  private def getVersionForPut(key: K) = {
    var version: Version = getVersionWithResolution(key)
    if (version == null) version = new VectorClock
    version
  }
}