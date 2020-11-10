package org.dist.partitioning

import java.util

class Storage {
  val map = new util.HashMap[String, String]()
}

class KeyRangePartitioning {
  val storages = List(new Storage, new Storage, new Storage)

  def put(key:String, value:String): Unit = {
    val index = partitionFor(key)
    val storage = storages(index)
    storage.map.put(key, value)
  }

  def partitionFor(key:String) = {
    0
  }


}
