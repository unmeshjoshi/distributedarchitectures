package org.dist.kvstore

import java.util

class ArrivalWindow(size:Int) {
  private var tLast_ = 0d
  private var arrivalIntervals_ = new util.ArrayList[Double](size)

  private[kvstore] def add(value: Double): Unit = {
    if (arrivalIntervals_.size == size) arrivalIntervals_.remove(0)
    var interArrivalTime = 0d
    if (tLast_ > 0d) interArrivalTime = value - tLast_
    tLast_ = value
    arrivalIntervals_.add(interArrivalTime)
  }

  private[kvstore] def sum = {
    var sum = 0d
    val size = arrivalIntervals_.size
    for (i <- 0 until size) {
      sum += arrivalIntervals_.get(i)
    }
    sum
  }

  private[kvstore] def sumOfDeviations = {
    var sumOfDeviations = 0d
    val size = arrivalIntervals_.size
    for (i <- 0 until size) {
      sumOfDeviations += (arrivalIntervals_.get(i) - mean) * (arrivalIntervals_.get(i) - mean)
    }
    sumOfDeviations
  }

  private[kvstore] def mean = sum / arrivalIntervals_.size

  private[kvstore] def variance = sumOfDeviations / arrivalIntervals_.size

  private[kvstore] def deviation = Math.sqrt(variance)

  private[kvstore] def clear(): Unit = {
    arrivalIntervals_.clear()
  }

  private[kvstore] def p(t: Double) = { // Stat stat = new Stat();
    /* Exponential CDF = 1 -e^-lambda*x */
    val exponent = (-1) * t / mean
    1 - (1 - Math.pow(Math.E, exponent))
    // return stat.gaussianCDF(mean, deviation, t, Double.POSITIVE_INFINITY);
  }

  private[kvstore] def phi(tnow: Long) = {
    val size = arrivalIntervals_.size
    var log = 0d
    if (size > 0) {
      val t = tnow - tLast_
      val probability = p(t)
      log = (-1) * Math.log10(probability)
    }
    log
  }
}