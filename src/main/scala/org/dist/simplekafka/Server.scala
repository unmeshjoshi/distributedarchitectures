package org.dist.simplekafka

import org.dist.queue.server.Config

class Server(val config:Config, val zookeeperClient: ZookeeperClient, val controller:Controller, val socketServer: SimpleSocketServer) {
  def startup() = {
    socketServer.startup()
    zookeeperClient.registerSelf()
    controller.elect()
  }
}
