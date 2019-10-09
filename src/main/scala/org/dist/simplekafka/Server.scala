package org.dist.simplekafka

import org.dist.queue.server.Config

class Server(config:Config) {
  var zookeeperClient:ZookeeperClient = null
  def startup() = {
    zookeeperClient = new ZookeeperClientImpl(config)
  }
}
