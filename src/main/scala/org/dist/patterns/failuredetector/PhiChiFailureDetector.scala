package org.dist.patterns.failuredetector

import java.util
import java.util.concurrent.TimeUnit


object PhiChiFailureDetector {
  val INITIAL_VALUE_NANOS: Long = TimeUnit.NANOSECONDS.convert(getInitialValue, TimeUnit.MILLISECONDS)

  private def getInitialValue = {
    val intervalInMillis = 1000
    intervalInMillis * 2
  }
}

class PhiChiFailureDetector[T] extends FailureDetector[T] {
  private val arrivalSamples = new util.Hashtable[T, ArrivalWindow[T]]
  private val SAMPLE_SIZE = 1000
  private val phiSuspectThreshold_ = 5
  private val phiConvictThreshold_ = 8
  /* The Failure Detector has to have been up for atleast 1 min. */
  private val uptimeThreshold_ = 60000
  private var lastInterpret = System.nanoTime()


  def report(ep: T): Unit = {
    val now = System.nanoTime()
    var heartbeatWindow = arrivalSamples.get(ep)
    if (heartbeatWindow == null) { // avoid adding an empty ArrivalWindow to the Map
      heartbeatWindow = new ArrivalWindow(SAMPLE_SIZE)
      heartbeatWindow.add(now, ep)
      heartbeatWindow = arrivalSamples.putIfAbsent(ep, heartbeatWindow)
      if (heartbeatWindow != null) heartbeatWindow.add(now, ep)
    }
    else heartbeatWindow.add(now, ep)
    if (logger.isTraceEnabled && heartbeatWindow != null) logger.trace("Average for {} is {}ns", ep, heartbeatWindow.mean)
  }

  private val DEFAULT_MAX_PAUSE = 5000L * 1000000L // 5 seconds

  private val MAX_LOCAL_PAUSE_IN_NANOS = DEFAULT_MAX_PAUSE
  private val PHI_FACTOR = 1.0 / Math.log(10.0) // 0.434...
  private val DEBUG_PERCENTAGE = 80 // if the phi is larger than this percentage of the max, log a debug message


  private var lastPause = 0L

  def getPhiConvictThreshold = {
     // return DatabaseDescriptor.getPhiConvictThreshold//
//    8.0
    3.0 //with 3, the crash is detected in 15 seconds.TODO figure out why
  }

  def interpret(ep: T): Unit = {
    val hbWnd = arrivalSamples.get(ep)
    if (hbWnd == null) return
    val now = System.nanoTime()
    val diff = now - lastInterpret
    lastInterpret = now
    if (diff > MAX_LOCAL_PAUSE_IN_NANOS) {
      logger.warn("Not marking nodes down due to local pause of {}ns > {}ns", diff, MAX_LOCAL_PAUSE_IN_NANOS)
      lastPause = now
      return
    }
    if (System.nanoTime() - lastPause < MAX_LOCAL_PAUSE_IN_NANOS) {
      logger.debug("Still not marking nodes down due to local pause")
      return
    }
    val phi = hbWnd.phi(now)
    if (logger.isTraceEnabled) {
      logger.trace(s"PHI for ${ep} : ${phi}")
      logger.trace(s"PHI_FACTOR * phi for ${ep} : ${PHI_FACTOR * phi} and PhiConvictThreshold:${getPhiConvictThreshold}")
    }
    if (PHI_FACTOR * phi > getPhiConvictThreshold) {
      if (logger.isTraceEnabled) trace(s"Node ${ep} phi ${PHI_FACTOR * phi} > ${getPhiConvictThreshold}; intervals: ${hbWnd} mean: ${hbWnd.mean}ns")
      markDown(ep)

    }
    else if (logger.isDebugEnabled && (PHI_FACTOR * phi * DEBUG_PERCENTAGE / 100.0 > getPhiConvictThreshold))
      logger.debug(s"PHI for ${ep} : ${phi}")
    else if (logger.isTraceEnabled) {
      logger.debug(s"PHI for ${ep} : ${phi}")
      logger.trace(s"mean for ${ep} : ${hbWnd.mean}ns")
    }
  }

  override def heartBeatCheck(): Unit = {
    val keys = arrivalSamples.keySet()
    keys.forEach(key ⇒ interpret(key))
  }

  override def heartBeatReceived(serverId: T): Unit = {
    report(serverId)
    super.markUp(serverId)
  }
}


