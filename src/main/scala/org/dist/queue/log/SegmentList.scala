package org.dist.queue.log

import java.util.concurrent.atomic._

import org.dist.queue.common.KafkaException

import scala.math._
import scala.reflect._

private[log] object SegmentList {
  val MaxAttempts: Int = 20
}

/**
 * A copy-on-write list implementation that provides consistent views. The view() method
 * provides an immutable sequence representing a consistent state of the list. The user can do
 * iterative operations on this sequence such as binary search without locking all access to the list.
 * Even if the range of the underlying list changes no change will be made to the view
 */
private[log] class SegmentList[T](seq: Seq[T])(implicit m: ClassManifest[T]) {

  val contents: AtomicReference[Array[T]] = new AtomicReference(seq.toArray)

  /**
   * Append the given items to the end of the list
   */
  def append(ts: T*)(implicit m: ClassManifest[T]) {
    val curr = contents.get()
    val updated = new Array[T](curr.length + ts.length)
    Array.copy(curr, 0, updated, 0, curr.length)
    for(i <- 0 until ts.length)
      updated(curr.length + i) = ts(i)
    contents.set(updated)
  }


  /**
   * Delete the first n items from the list
   */
  def trunc(newStart: Int): Seq[T] = {
    if(newStart < 0)
      throw new KafkaException("Starting index must be positive.");
    var deleted: Array[T] = null
    val curr = contents.get()
    if (curr.length > 0) {
      val newLength = max(curr.length - newStart, 0)
      val updated = new Array[T](newLength)
      Array.copy(curr, min(newStart, curr.length - 1), updated, 0, newLength)
      contents.set(updated)
      deleted = new Array[T](newStart)
      Array.copy(curr, 0, deleted, 0, curr.length - newLength)
    }
    deleted
  }

  /**
   * Delete the items from position (newEnd + 1) until end of list
   */
  def truncLast(newEnd: Int): Seq[T] = {
    if (newEnd < 0 || newEnd >= contents.get().length)
      throw new KafkaException("Attempt to truncate segment list of length %d to %d.".format(contents.get().size, newEnd));
    var deleted: Array[T] = null
    val curr = contents.get()
    if (curr.length > 0) {
      val newLength = newEnd + 1
      val updated = new Array[T](newLength)
      Array.copy(curr, 0, updated, 0, newLength)
      contents.set(updated)
      deleted = new Array[T](curr.length - newLength)
      Array.copy(curr, min(newEnd + 1, curr.length - 1), deleted, 0, curr.length - newLength)
    }
    deleted
  }

  /**
   * Get a consistent view of the sequence
   */
  def view: Array[T] = contents.get()

  /**
   * Nicer toString method
   */
  override def toString(): String = "SegmentList(%s)".format(view.mkString(", "))

}
