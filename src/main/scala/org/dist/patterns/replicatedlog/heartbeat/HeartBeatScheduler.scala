package org.dist.patterns.replicatedlog.heartbeat

import java.util.Random
import java.util.concurrent.{Future, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import org.dist.queue.common.Logging


class HeartBeatTask(action:()⇒Unit) extends Runnable with Logging {
  override def run(): Unit = {
    action()
  }
}

class HeartBeatScheduler(action:()⇒Unit) extends Logging {

  val heartbeat_interval = 30

  val election_min_interval = 150
  
  val election_max_interval = 300


  val executor = new ScheduledThreadPoolExecutor(1);

  var scheduledGossipTask:ScheduledFuture[_] = _

  def start(): Unit = {
    scheduledGossipTask = executor.scheduleWithFixedDelay(new HeartBeatTask(action), heartbeat_interval, heartbeat_interval, TimeUnit.MILLISECONDS)
    info(s"Starting hearbeat task ${heartbeat_interval} ms")
  }

  def startWithRandomInterval(): Unit = {
    val nextInterval = computeElectionTimeout(election_min_interval, election_max_interval)
    info(s"Starting random election timeout ${nextInterval} ms in")
    scheduledGossipTask = executor.scheduleWithFixedDelay(new HeartBeatTask(action), nextInterval, nextInterval, TimeUnit.MILLISECONDS)
  }

  def cancel() = {
    if (scheduledGossipTask != null) {
      scheduledGossipTask.cancel(true)
    }
  }

  private def computeElectionTimeout(min: Int, max: Int): Int = {
    val diff = max - min
    new Random().nextInt(diff + 1) + min
  }
}
