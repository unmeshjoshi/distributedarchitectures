package org.dist.queue.client.consumer

import org.dist.queue.api.OffsetRequest

object ConsumerConfig {
  val DefaultClientId: String = ""

  val RefreshMetadataBackoffMs = 200
  val SocketTimeout = 30 * 1000
  val SocketBufferSize = 64*1024
  val FetchSize = 1024 * 1024
  val MaxFetchSize = 10*FetchSize
  val DefaultFetcherBackoffMs = 1000
  val AutoCommit = true
  val AutoCommitInterval = 60 * 1000
  val MaxQueuedChunks = 2
  val MaxRebalanceRetries = 4
  val AutoOffsetReset = OffsetRequest.LargestTimeString
  val ConsumerTimeoutMs = -1
  val MinFetchBytes = 1
  val MaxFetchWaitMs = 100
  val MirrorTopicsWhitelist = ""
  val MirrorTopicsBlacklist = ""
}
