package org.dist.dbgossip

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

object Db extends App {
  val intervalInMillis = 1000
  val executor = new ScheduledThreadPoolExecutor(1)
  val scheduledGossipTask = executor.scheduleWithFixedDelay(new GossipTask, intervalInMillis, intervalInMillis, TimeUnit.MILLISECONDS)

  private class GossipTask extends Runnable {
    override def run(): Unit = {

    }
  }

}
