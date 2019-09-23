package org.dist.queue

import org.dist.util.Networks

case class Config(brokerId: Int,
                  hostName: String,
                  port: Int,
                  zkConnect: String,
                  zkSessionTimeoutMs: Int = 6000,
                  zkConnectionTimeoutMs: Int = 6000)

