package org.dist.queue.common

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}

import org.dist.queue.utils.Utils

import scala.collection.mutable.HashMap


/**
 * A scheduler for running jobs in the background
 */
class KafkaScheduler(val numThreads: Int) extends Logging {
  private var executor:ScheduledThreadPoolExecutor = null
  private val daemonThreadFactory = new ThreadFactory() {
    def newThread(runnable: Runnable): Thread = Utils.newThread(runnable, true)
  }
  private val nonDaemonThreadFactory = new ThreadFactory() {
    def newThread(runnable: Runnable): Thread = Utils.newThread(runnable, false)
  }
  private val threadNamesAndIds = new HashMap[String, AtomicInteger]()

  def startup() = {
    executor = new ScheduledThreadPoolExecutor(numThreads)
    executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false)
    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
  }

  def hasShutdown: Boolean = executor.isShutdown

  private def ensureExecutorHasStarted = {
    if(executor == null)
      throw new IllegalStateException("Kafka scheduler has not been started")
  }

  def scheduleWithRate(fun: () => Unit, name: String, delayMs: Long, periodMs: Long, isDaemon: Boolean = true) = {
    ensureExecutorHasStarted
    if(isDaemon)
      executor.setThreadFactory(daemonThreadFactory)
    else
      executor.setThreadFactory(nonDaemonThreadFactory)
    val threadId = threadNamesAndIds.getOrElseUpdate(name, new AtomicInteger(0))
    executor.scheduleAtFixedRate(Utils.loggedRunnable(fun, name + threadId.incrementAndGet()), delayMs, periodMs,
      TimeUnit.MILLISECONDS)
  }

  def shutdownNow() {
    ensureExecutorHasStarted
    executor.shutdownNow()
    info("Forcing shutdown of Kafka scheduler")
  }

  def shutdown() {
    ensureExecutorHasStarted
    executor.shutdown()
    info("Shutdown Kafka scheduler")
  }
}

