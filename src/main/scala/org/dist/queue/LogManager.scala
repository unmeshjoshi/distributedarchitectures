package org.dist.queue

class LogManager(val config: Config,
                         private val time: Time) extends Logging {
  private val logs = new Pool[TopicAndPartition, Log]()

  def getOrCreateLog(topic: String, partition: Int): Log = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    logs.get(topicAndPartition) match {
      case null => createLogIfNotExists(topicAndPartition)
      case log: Log => log
    }
  }

  /**
   * Create a log for the given topic and the given partition
   * If the log already exists, just return a copy of the existing log
   */
  private def createLogIfNotExists(topicAndPartition: TopicAndPartition): Log = {
    new Log
  }

  def startup() = {
    info("starting log manager")
  }
}
