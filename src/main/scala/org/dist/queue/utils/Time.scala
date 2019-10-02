package org.dist.queue.utils

/**
 * A mockable interface for time functions
 */
trait Time {

  def milliseconds: Long

  def nanoseconds: Long

  def sleep(ms: Long)
}

/**
 * The normal system implementation of time functions
 */
object SystemTime extends Time {

  def milliseconds: Long = System.currentTimeMillis

  def nanoseconds: Long = System.nanoTime

  def sleep(ms: Long): Unit = Thread.sleep(ms)

}
