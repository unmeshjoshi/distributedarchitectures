package org.dist.queue.common

import java.util.concurrent.atomic.AtomicReference

import org.I0Itec.zkclient.ZkClient
import org.dist.queue.server.Config
import org.dist.queue.utils.ZKStringSerializer

object KafkaZookeeperClient {
  private val INSTANCE = new AtomicReference[ZkClient](null)

  def getZookeeperClient(config: Config, connect: Config => String): ZkClient = {
    // TODO: This cannot be a singleton since unit tests break if we do that
    //    INSTANCE.compareAndSet(null, new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
    //                                              ZKStringSerializer))
    INSTANCE.set(new ZkClient(connect(config), config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
      ZKStringSerializer))
    INSTANCE.get()
  }
}
