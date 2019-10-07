package org.dist.queue.server

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

import org.dist.queue.common.{KafkaScheduler, Logging}
import org.dist.queue.controller.Controller
import org.dist.queue.log.LogManager
import org.dist.queue.network.SocketServer
import org.dist.queue.utils.{SystemTime, Time, Utils}

class Server(val config: Config, time: Time = SystemTime) extends Logging {
  private var shutdownLatch = new CountDownLatch(1)
  var kafkaZooKeeper:KafkaZooKeeper = _
  var controller:Controller = _
  var replicaManager: ReplicaManager = _
  var logManager: LogManager = null
  var kafkaScheduler:KafkaScheduler = null

  var isShuttingDown:AtomicBoolean = _

  var socketServer:SocketServer = _
  var apis: KafkaApis = null

  def startup(): Unit = {
    isShuttingDown = new AtomicBoolean(false)
    kafkaScheduler = new KafkaScheduler(1)
    kafkaScheduler.startup()

    logManager = new LogManager(config,kafkaScheduler, time)
    logManager.startup()

    socketServer = new SocketServer(config.brokerId,
      config.hostName,
      config.port,
      config.numNetworkThreads,
      config.queuedMaxRequests,
      config.socketSendBufferBytes,
      config.socketReceiveBufferBytes,
      config.socketRequestMaxBytes)



    kafkaZooKeeper = new KafkaZooKeeper(config)
    kafkaZooKeeper.startup()

    controller = new Controller(config, kafkaZooKeeper.getZookeeperClient, socketServer)


    replicaManager = new ReplicaManager(config, time, kafkaZooKeeper.getZookeeperClient, logManager, isShuttingDown)
    replicaManager.startup()

    apis = new KafkaApis(replicaManager, kafkaZooKeeper.getZookeeperClient, config.brokerId, controller)
    socketServer.startup(apis)

    controller.startup()
  }

  def controlledShutdown(): Unit = {
  }


  def shutdown(): Unit = {
    info("Shutting down")
    val canShutdown = isShuttingDown.compareAndSet(false, true);
    if (canShutdown) {
      Utils.swallow(controlledShutdown())
      if (kafkaZooKeeper != null)
        Utils.swallow(kafkaZooKeeper.shutdown())
      if (socketServer != null)
        Utils.swallow(socketServer.shutdown())
      Utils.swallow(kafkaScheduler.shutdown())
      if (apis != null)
        Utils.swallow(apis.close())
      if (replicaManager != null)
        Utils.swallow(replicaManager.shutdown())
      if (logManager != null)
        Utils.swallow(logManager.shutdown())

      if (controller != null)
        Utils.swallow(controller.shutdown())

      shutdownLatch.countDown()
      //      startupComplete.set(false);
      info("Shut down completed")
    }
  }
  def awaitShutdown(): Unit = shutdownLatch.await()
}
