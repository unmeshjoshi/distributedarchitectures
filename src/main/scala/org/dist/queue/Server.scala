package org.dist.queue

class Server(config: Config) {

  def startup(): Unit = {
    val kafkaZooKeeper = new KafkaZooKeeper(config)
    kafkaZooKeeper.startup()
  }
}
