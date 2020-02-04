package org.dist.patterns.replicatedlog.heartbeat

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import org.dist.queue.common.Logging

class HeartBeatTask extends Runnable with Logging {
  //heartbeat time is 100 ms
  override def run(): Unit = {
    info("Sending heartbeats")
  }
}
class HeartBeat {
  val executor = new ScheduledThreadPoolExecutor(1);


  var scheduledGossipTask = new HeartBeatTask()

  def start(): Unit = {
    scheduledGossipTask = executor.scheduleWithFixedDelay(scheduledGossipTask, 100, 100, TimeUnit.MILLISECONDS)

  }

}
