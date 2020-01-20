package org.dist.versionedkvstore

import java.util
import java.util.{Collections, List}

//represents server
class Node[K, V](val id:Int,
                 partitions: util.List[Int] = Collections.emptyList(),
                 host: String = "localhost",
                 httpPort: Int = 0,
                 socketPort: Int = 0,
                 adminPort: Int = 0) {


  def put(key: K, versionedCopy: Versioned[V]) = {
    map.put(key, versionedCopy)
  }

  val map = new VersionedMap[K, V]()
  def get(key:K) = map.get(key)
  def getVersions(key:K) = map.getVersions(key)

  import scala.jdk.CollectionConverters._
  def getPartitionIds():util.List[Int] = partitions.asScala.asJava
}

