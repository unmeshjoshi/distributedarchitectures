package org.dist.queue

import java.util.concurrent.atomic.AtomicBoolean

class Server(config: Config, time: Time = SystemTime) {
  var kafkaZooKeeper:KafkaZooKeeper = _
  var controller:Controller = _
  var replicaManager: ReplicaManager = _
  var logManager: LogManager = null

  var isShuttingDown:AtomicBoolean = _

  def startup(): Unit = {
    isShuttingDown = new AtomicBoolean(false)
    
    logManager = new LogManager(config, time)
    logManager.startup()


    kafkaZooKeeper = new KafkaZooKeeper(config)
    kafkaZooKeeper.startup()


    replicaManager = new ReplicaManager(config, time, kafkaZooKeeper.getZookeeperClient, logManager, isShuttingDown)
    replicaManager.startup()

    controller = new Controller(config, kafkaZooKeeper.getZookeeperClient)
    controller.startup()
  }
}
