package org.dist.simplekafka

import org.dist.queue.server.Config

class Server(config:Config, zookeeperClient: ZookeeperClient, controller:Controller) {
  def startup() = {
    zookeeperClient.registerSelf()
    controller.elect()
  }
}
