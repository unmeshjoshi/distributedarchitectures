package org.dist.queue.controller

import org.I0Itec.zkclient.IZkDataListener
import org.dist.queue.common.Logging
import org.dist.queue.utils.ZkUtils

class AddPartitionsListener(topic: String, controller:Controller) extends IZkDataListener with Logging {
  private val controllerContext = controller.controllerContext
  private val zkClient = controllerContext.zkClient

  this.logIdent = "[AddPartitionsListener on " + controller.config.brokerId + "]: "

  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    controllerContext.controllerLock synchronized {
      try {
        info("Add Partition triggered " + data.toString + " for path " + dataPath)
        val partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, List(topic))
        val partitionsRemainingToBeAdded = partitionReplicaAssignment.filter(p =>
          !controllerContext.partitionReplicaAssignment.contains(p._1))
        info("New partitions to be added [%s]".format(partitionsRemainingToBeAdded))
        controller.onNewPartitionCreation(partitionsRemainingToBeAdded.keySet.toSet)
      } catch {
        case e: Throwable => error("Error while handling add partitions for data path " + dataPath, e)
      }
    }
  }

  @throws(classOf[Exception])
  def handleDataDeleted(parentPath: String) {
    // this is not implemented for partition change
  }
}