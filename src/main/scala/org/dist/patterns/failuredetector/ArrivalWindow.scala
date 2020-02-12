package org.dist.patterns.failuredetector


import java.util
import java.util.concurrent.TimeUnit

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.common.Logging
import org.slf4j.{Logger, LoggerFactory}

/*
 This class is not thread safe.
 */
class ArrayBackedBoundedStats(val sizeInt: Int) {
  val arrivalIntervals = new Array[Long](sizeInt)
  private var sum: Long = 0
  private var index: Int = 0
  private var isFilled: Boolean = false
  var mean: Double = 0

  def add(interval: Long): Unit = {
    if (index == arrivalIntervals.length) {
      isFilled = true
      index = 0
    }
    if (isFilled) sum = sum - arrivalIntervals(index)
    arrivalIntervals({
      index += 1; index - 1
    }) = interval
    sum += interval
    mean = sum.toDouble / size
  }

  private def size: Int =
    if (isFilled)
      arrivalIntervals.length
    else
      index

  def getArrivalIntervals: Array[Long] = arrivalIntervals
}

object ArrivalWindow {

  private def getMaxInterval: Long = {
    PhiChiFailureDetector.INITIAL_VALUE_NANOS
  }
}

class ArrivalWindow[T](val size: Int) extends Logging {
  arrivalIntervals = new ArrayBackedBoundedStats(size)
  private var tLast: Long = 0L
  final private var arrivalIntervals: ArrayBackedBoundedStats = new ArrayBackedBoundedStats(size)
  private var lastReportedPhi: Double = Double.MinValue
  // in the event of a long partition, never record an interval longer than the rpc timeout,
  // since if a host is regularly experiencing connectivity problems lasting this long we'd
  // rather mark it down quickly instead of adapting
  // this value defaults to the same initial value the FD is seeded with
  final private val MAX_INTERVAL_IN_NANO: Long = ArrivalWindow.getMaxInterval

  private[failuredetector] def add(value: Long, ep: T): Unit = {
    assert(tLast >= 0)
    if (tLast > 0L) {
      val interArrivalTime: Long = value - tLast
      if (interArrivalTime <= MAX_INTERVAL_IN_NANO) {
        arrivalIntervals.add(interArrivalTime)
        trace("Reporting interval time of ${interArrivalTime}ns for ${ep}")
      }
      else trace(s"Ignoring interval time of ${interArrivalTime}ns for ${ep}")
    }
    else { // We use a very large initial interval since the "right" average depends on the cluster size
      // and it's better to err high (false negatives, which will be corrected by waiting a bit longer)
      // than low (false positives, which cause "flapping").
      arrivalIntervals.add(PhiChiFailureDetector.INITIAL_VALUE_NANOS)
    }
    tLast = value
  }

  private[failuredetector] def mean: Double = arrivalIntervals.mean

  // see CASSANDRA-2597 for an explanation of the math at work here.
  private[failuredetector] def phi(tnow: Long): Double = {
    assert(arrivalIntervals.mean > 0 && tLast > 0) // should not be called before any samples arrive

    val t: Long = tnow - tLast
    lastReportedPhi = t / mean
    lastReportedPhi
  }

  private[failuredetector] def getLastReportedPhi: Double = lastReportedPhi

  override def toString: String = util.Arrays.toString(arrivalIntervals.getArrivalIntervals)
}

