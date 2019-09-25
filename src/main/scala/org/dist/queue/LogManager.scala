package org.dist.queue

class LogManager(val config: Config,
                         private val time: Time) extends Logging {
  def startup() = {
    info("starting log manager")
  }
}
