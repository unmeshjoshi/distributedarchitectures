package org.dist.simplekafka

import org.dist.queue.common.Logging
import org.dist.queue.server.Config

class Server(val config:Config, val zookeeperClient: ZookeeperClient, val controller:Controller, val socketServer: SimpleSocketServer) extends Logging {
  def startup() = {
    socketServer.startup()
    zookeeperClient.registerSelf()
    controller.startup()

    info(s"Server ${config.brokerId} started with log dir ${config.logDirs}")
  }

  def shutdown()= {
    zookeeperClient.shutdown()
    socketServer.shutdown()
  }
}
