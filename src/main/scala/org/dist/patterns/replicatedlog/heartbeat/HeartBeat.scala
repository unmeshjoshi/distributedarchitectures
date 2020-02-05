package org.dist.patterns.replicatedlog.heartbeat

import java.util.concurrent.{Future, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import org.dist.queue.common.Logging

class HeartBeatTask extends Runnable with Logging {
  //heartbeat time is 100 ms
  override def run(): Unit = {
    info("Sending heartbeats")
  }
}
class HeartBeat {
  val executor = new ScheduledThreadPoolExecutor(1);


  var scheduledGossipTask:ScheduledFuture[_] = _

  def start(): Unit = {
    scheduledGossipTask = executor.scheduleWithFixedDelay(new HeartBeatTask(), 100, 100, TimeUnit.MILLISECONDS)

  }

}
