package org.dist.queue

class Server(config: Config) {
  var kafkaZooKeeper:KafkaZooKeeper = _
  var controller:Controller = _

  def startup(): Unit = {
    kafkaZooKeeper = new KafkaZooKeeper(config)
    kafkaZooKeeper.startup()

    controller = new Controller(config, kafkaZooKeeper.getZookeeperClient)
    controller.startUp()
  }
}
