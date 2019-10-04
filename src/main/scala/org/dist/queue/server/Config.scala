package org.dist.queue.server

import org.dist.queue.message.NoCompressionCodec

import scala.collection.Map

case class Config(brokerId: Int,
                  hostName: String,
                  port: Int,
                  zkConnect: String,
                  val logDirs:List[String],
                  zkSessionTimeoutMs: Int = 6000,
                  zkConnectionTimeoutMs: Int = 6000,
                  controllerMessageQueueSize:Int = 10,
                  val controllerSocketTimeoutMs: Int = 10
                  ) {
  val offsetsTopicNumPartitions: Int = 50


  val FetchSize = 1024 * 1024

  val compressionCodec = NoCompressionCodec

  val messageMaxBytes: Int = 1000000

  val defaultReplicationFactor = 1

  val numPartitions = 1

  val autoCreateTopicsEnable: Boolean = false

  val numIoThreads: Int = 8


  val socketRequestMaxBytes: Int = 100*1024*1024

  val socketReceiveBufferBytes: Int = 100*1024

  val socketSendBufferBytes: Int = 100*1024

  val queuedMaxRequests: Int = 500

  val numNetworkThreads: Int = 3

  /* the directories in which the log data is kept */
  require(logDirs.size > 0)

  /* the maximum size of a single log file */
  val logSegmentBytes: Int = 1*1024*1024*1024

  /* the maximum size of a single log file for some specific topic */
  val logSegmentBytesPerTopicMap = Map[String, Int]()

  /* the maximum time before a new log segment is rolled out */
  val logRollHours = 1

  /* the number of hours before rolling out a new log segment for some specific topic */
  val logRollHoursPerTopicMap = Map[String, String]()

  /* the number of hours to keep a log file before deleting it */
  val logRetentionHours = 1

  /* the number of hours to keep a log file before deleting it for some specific topic*/
  val logRetentionHoursPerTopicMap = Map[String, Int]()

  /* the maximum size of the log before deleting it */
  val logRetentionBytes = 1

  /* the maximum size of the log for some specific topic before deleting it */
  val logRetentionBytesPerTopicMap = Map[String, Int]()

  /* the frequency in minutes that the log cleaner checks whether any log is eligible for deletion */
  val logCleanupIntervalMins = 10

  /* the maximum size in bytes of the offset index */
  val logIndexSizeMaxBytes = 10*1024*1024

  /* the interval with which we add an entry to the offset index */
  val logIndexIntervalBytes = 4096

  /* the number of messages accumulated on a log partition before messages are flushed to disk */
  val logFlushIntervalMessages = 10000

  /* the maximum time in ms that a message in selected topics is kept in memory before flushed to disk, e.g., topic1:3000,topic2: 6000  */
  val logFlushIntervalMsPerTopicMap = Map[String, Int]()

  /* the frequency in ms that the log flusher checks whether any log needs to be flushed to disk */
  val logFlushSchedulerIntervalMs =3000

  /* the maximum time in ms that a message in any topic is kept in memory before flushed to disk */
  val logFlushIntervalMs = logFlushSchedulerIntervalMs


}
