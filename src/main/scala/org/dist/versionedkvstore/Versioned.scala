package org.dist.versionedkvstore

import java.io.Serializable
import java.util.Comparator

@SerialVersionUID(1)
object Versioned {
  def value[S](s: S) = new Versioned[S](s, new VectorClock)

  def value[S](s: S, v: Version) = new Versioned[S](s, v)

  final class HappenedBeforeComparator[S] extends Comparator[Versioned[S]] {
    def compare(v1: Versioned[S], v2: Versioned[S]): Int = {
      val occurred = v1.getVersion.compare(v2.getVersion)
      if (occurred eq Occurred.BEFORE) -1
      else if (occurred eq Occurred.AFTER) 1
      else 0
    }
  }

}

@SerialVersionUID(1)
case class Versioned[T](var value: T, val version: Version) extends Serializable {

  def this(value: T) {
    this(value, new VectorClock)
  }

  def getVersion: Version = version

  def getValue: T = value

  def setObject(value: T): Unit = {
    this.value = value
  }
}

