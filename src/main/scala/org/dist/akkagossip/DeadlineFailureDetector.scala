package org.dist.akkagossip

import scala.concurrent.duration.{Duration, FiniteDuration, NANOSECONDS}


class DeadlineFailureDetector(
                               val acceptableHeartbeatPause: FiniteDuration,
                               val heartbeatInterval:        FiniteDuration) extends  FailureDetector {


  /**
   * Abstraction of a clock that returns time in milliseconds. Clock can only be used to measure elapsed
   * time and is not related to any other notion of system or wall-clock time.
   */
  // Abstract class to be able to extend it from Java

  require(acceptableHeartbeatPause >= Duration.Zero, "failure-detector.acceptable-heartbeat-pause must be >= 0 s")
  require(heartbeatInterval > Duration.Zero, "failure-detector.heartbeat-interval must be > 0 s")

  private val deadlineMillis = acceptableHeartbeatPause.toMillis + heartbeatInterval.toMillis
  @volatile private var heartbeatTimestamp = 0L //not used until active (first heartbeat)
  @volatile private var active = true

  def isAvailable: Boolean = isAvailable(clock())

  private def isAvailable(timestamp: Long): Boolean =
    if (active) (heartbeatTimestamp + deadlineMillis) > timestamp
    else true // treat unmanaged connections, e.g. with zero heartbeats, as healthy connections

  def isMonitoring: Boolean = active

  def clock(): Long = {
    NANOSECONDS.toMillis(System.nanoTime)
  }

  def heartbeat(): Unit = {
    heartbeatTimestamp = clock()
    active = true
  }

  override def remove(): Unit = {}
}