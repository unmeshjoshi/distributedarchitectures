package org.dist.versionedkvstore

import java.util

import org.scalatest.FunSuite

class VersionedMapTest extends FunSuite {

  test("should get empty version for new key") {
    val map = new VersionedMap[String, String]()
    assert(0 == map.get("newKey").size())
  }

  class Client[K, V] {
    val metadataRefreshAttempts: Int = 2

    val node1 = new Node[K, V](1)
    val node2 = new Node[K, V](2)

    def put(key: K, value: V): Version = {
      val master = node1 //assume node1 is always the master and node2 is replica
      val version = getVersionForPut(key)
      val versioned = Versioned.value(value, version)
      val versionedClock = versioned.getVersion.asInstanceOf[VectorClock]

      val versionedCopy = new Versioned[V](versioned.getValue, versionedClock.incremented(master.id, System.currentTimeMillis()))
      master.put(key, versionedCopy)

      versionedCopy.getVersion
    }

    import scala.jdk.CollectionConverters._

    protected def getItemOrThrow(key: K, items: util.List[Versioned[V]]): Versioned[V] = {
      if (items.size == 0) null //default Value
      else if (items.size == 1) items.get(0)
      else throw new InconsistentDataException("Unresolved versions returned from get(" + key + ") = " + items, items.asInstanceOf[java.util.List[Versioned[_]]])
    }

    protected var storeName: String = "defaultStore"
    def get(key: K): Versioned[V] = {
      for (attempts <- 0 until this.metadataRefreshAttempts) {
        try {
          val items: util.List[Versioned[V]] = node1.get(key)
          val resolvedItems = new VectorClockInconsistencyResolver[V]().resolveConflicts(items)
          return getItemOrThrow(key,resolvedItems)
        } catch {
          case e:Exception ⇒
            info("Received invalid metadata exception during get [  " + e.getMessage + " ] on store '" + storeName + "'. Rebootstrapping")
            bootStrap()
        }
      }
      throw new RuntimeException(this.metadataRefreshAttempts + " metadata refresh attempts failed.")
    }

    def bootStrap (): Unit = {

    }

    def getVersionWithResolution(key: K) = {
      val nodes = List[Node[K, V]](node1, node2)
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
      var version:Version = getVersionWithResolution(key)
      if (version == null) version = new VectorClock
      version
    }
  }
  //represents server
  case class Node[K, V](id:Int) {
    def put(key: K, versionedCopy: Versioned[V]) = {
      map.put(key, versionedCopy)
    }

    val map = new VersionedMap[K, V]()
    def get(key:K) = map.get(key)
    def getVersions(key:K) = map.getVersions(key)
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
