package org.dist.queue

import java.util.concurrent.atomic.AtomicBoolean

import org.dist.queue.network.SocketServer

class Server(config: Config, time: Time = SystemTime) {
  var kafkaZooKeeper:KafkaZooKeeper = _
  var controller:Controller = _
  var replicaManager: ReplicaManager = _
  var logManager: LogManager = null
  var requestHandlerPool: KafkaRequestHandlerPool = null

  var isShuttingDown:AtomicBoolean = _

  var socketServer:SocketServer = _
  var apis: KafkaApis = null

  def startup(): Unit = {
    isShuttingDown = new AtomicBoolean(false)
    
    logManager = new LogManager(config, time)
    logManager.startup()

    socketServer = new SocketServer(config.brokerId,
      config.hostName,
      config.port,
      config.numNetworkThreads,
      config.queuedMaxRequests,
      config.socketSendBufferBytes,
      config.socketReceiveBufferBytes,
      config.socketRequestMaxBytes)

    socketServer.startup()

    kafkaZooKeeper = new KafkaZooKeeper(config)
    kafkaZooKeeper.startup()

    controller = new Controller(config, kafkaZooKeeper.getZookeeperClient)

    apis = new KafkaApis(socketServer.requestChannel, replicaManager, kafkaZooKeeper.getZookeeperClient, config.brokerId, controller)

    requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads)

    replicaManager = new ReplicaManager(config, time, kafkaZooKeeper.getZookeeperClient, logManager, isShuttingDown)
    replicaManager.startup()

    controller.startup()
  }
}
