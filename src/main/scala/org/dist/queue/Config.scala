package org.dist.queue

import org.dist.util.Networks

case class Config(brokerId: Int,
                  hostName: String,
                  port: Int,
                  zkConnect: String,
                  zkSessionTimeoutMs: Int = 6000,
                  zkConnectionTimeoutMs: Int = 6000,
                  controllerMessageQueueSize:Int = 10,
                  val controllerSocketTimeoutMs: Int = 10) {
  val defaultReplicationFactor = 1

  val numPartitions = 1

  val autoCreateTopicsEnable: Boolean = false

  val numIoThreads: Int = 8


  val socketRequestMaxBytes: Int = 100*1024*1024

  val socketReceiveBufferBytes: Int = 100*1024

  val socketSendBufferBytes: Int = 100*1024

  val queuedMaxRequests: Int = 500

  val numNetworkThreads: Int = 3


}

