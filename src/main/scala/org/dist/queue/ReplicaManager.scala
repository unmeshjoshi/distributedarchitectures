package org.dist.queue

import java.util.concurrent.atomic.AtomicBoolean

import org.I0Itec.zkclient.ZkClient

class ReplicaManager(val config: Config,
                     time: Time,
                     val zkClient: ZkClient,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean ) extends Logging {
  def startup() = {

  }


}
