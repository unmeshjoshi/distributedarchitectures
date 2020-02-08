package org.dist.kvstore

import java.util

import org.dist.queue.common.Logging

object FailureDetector {
  def isAlive(sp:InetAddressAndPort) = true
}

class FailureDetector(gossiper: Gossiper) extends Logging {
  private val arrivalSamples_ = new util.Hashtable[InetAddressAndPort, ArrivalWindow]
  private val sampleSize_ = 1000
  private val phiSuspectThreshold_ = 5
  private val phiConvictThreshold_ = 8
  /* The Failure Detector has to have been up for atleast 1 min. */
  private val uptimeThreshold_ = 60000

  def report(ep: InetAddressAndPort): Unit = {
    val now = System.currentTimeMillis
    var hbWnd = arrivalSamples_.get(ep)
    if (hbWnd == null) {
      hbWnd = new ArrivalWindow(sampleSize_)
      arrivalSamples_.put(ep, hbWnd)
    }
    hbWnd.add(now)
  }

  def intepret(ep: InetAddressAndPort): Unit = {
    val hbWnd = arrivalSamples_.get(ep)
    if (hbWnd == null) return
    val now = System.currentTimeMillis
    /* We need this so that we do not suspect a convict. */ val isConvicted = false
    val phi = hbWnd.phi(now)
    info("PHI for " + ep + " : " + phi)
    if (!isConvicted && phi > phiSuspectThreshold_) {
        gossiper.suspect(ep)
    }
  }
}


