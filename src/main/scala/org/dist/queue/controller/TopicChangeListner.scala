package org.dist.queue.controller

import java.util.concurrent.atomic.AtomicBoolean

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.common.Logging
import org.dist.queue.utils.ZkUtils

import scala.jdk.CollectionConverters._


/**
 * This is the zookeeper listener that triggers all the state transitions for a partition
 */
class TopicChangeListener(controller:Controller, hasPartitionStateMachineStarted:AtomicBoolean) extends IZkChildListener with Logging {
  private val controllerContext = controller.controllerContext
  private val zkClient = controllerContext.zkClient

  this.logIdent = "[TopicChangeListener on Controller " + controller.config.brokerId + "]: "

  @throws(classOf[Exception])
  def handleChildChange(parentPath: String, children: java.util.List[String]) {
    controllerContext.controllerLock synchronized {
      if (hasPartitionStateMachineStarted.get) {
        try {
          val currentChildren = {
            debug("Topic change listener fired for path %s with children %s".format(parentPath, children.asScala.mkString(",")))
            children.asScala.toSet
          }
          val newTopics = currentChildren -- controllerContext.allTopics
          val deletedTopics = controllerContext.allTopics -- currentChildren
          //        val deletedPartitionReplicaAssignment = replicaAssignment.filter(p => deletedTopics.contains(p._1._1))
          controllerContext.allTopics = currentChildren

          val addedPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, newTopics.toSeq)
          controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p =>
            !deletedTopics.contains(p._1.topic))
          controllerContext.partitionReplicaAssignment.++=(addedPartitionReplicaAssignment)
          info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
            deletedTopics, addedPartitionReplicaAssignment))
          if (newTopics.size > 0)
            controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet.toSet)
        } catch {
          case e: Throwable => error("Error while handling new topic", e)
        }
        // TODO: kafka-330  Handle deleted topics
      }
    }
  }
}
