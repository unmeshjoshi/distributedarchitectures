package org.dist.queue

import java.util.concurrent.atomic.AtomicBoolean

import collection._
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener}
import org.dist.queue.utils.ZkUtils

import scala.collection.JavaConverters._
import scala.collection.{Seq, Set, mutable}

class ControllerBrokerRequestBatch(controllerContext: ControllerContext, sendRequest: (Int, RequestOrResponse, (RequestOrResponse) => Unit) => Unit,
                                   controllerId: Int, clientId: String) {
  def sendRequestsToBrokers(epoch: Int, getAndIncrement: Int): Unit = {

  }

  def newBatch() = {

  }

}




sealed trait PartitionState { def state: Byte }
case object NewPartition extends PartitionState { val state: Byte = 0 }
case object OnlinePartition extends PartitionState { val state: Byte = 1 }
case object OfflinePartition extends PartitionState { val state: Byte = 2 }
case object NonExistentPartition extends PartitionState { val state: Byte = 3 }

case class PartitionStateMachine(controller: Controller) extends Logging {

  def registerPartitionChangeListener(topic: String) = {
    zkClient.subscribeDataChanges(ZkUtils.getTopicPath(topic), new AddPartitionsListener(topic))
  }

  var partitionState: mutable.Map[TopicAndPartition, PartitionState] = mutable.Map.empty
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkClient = controllerContext.zkClient
  private val hasStarted = new AtomicBoolean(false)
  private val noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext)
  val brokerRequestBatch = new ControllerBrokerRequestBatch(controller.controllerContext, controller.sendRequest,
    controllerId, controller.clientId)


  def startup() = {

  }



  def registerListeners() = {
    registerTopicChangeListener()
  }


  private def registerTopicChangeListener() = {
    zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, new TopicChangeListener())
  }


  private def assertValidPreviousStates(topicAndPartition: TopicAndPartition, fromStates: Seq[PartitionState],
                                        targetState: PartitionState) {
    if(!fromStates.contains(partitionState(topicAndPartition)))
      throw new IllegalStateException("Partition %s should be in the %s states before moving to %s state"
        .format(topicAndPartition, fromStates.mkString(","), targetState) + ". Instead it is in %s state"
        .format(partitionState(topicAndPartition)))
  }

  def assignReplicasToPartitions(topic: String, partition: Int) = {
    val assignedReplicas = ZkUtils.getReplicasForPartition(controllerContext.zkClient, topic, partition)
    controllerContext.partitionReplicaAssignment += TopicAndPartition(topic, partition) -> assignedReplicas
  }

  def handleStateChange(topic: String, partition: Int, targetState: PartitionState, leaderSelector: PartitionLeaderSelector) = {

    val topicAndPartition = TopicAndPartition(topic, partition)
    if (!hasStarted.get)
      throw new StateChangeFailedException(("Controller %d epoch %d initiated state change for partition %s to %s failed because " +
        "the partition state machine has not started")
        .format(controllerId, controller.epoch, topicAndPartition, targetState))
    val currState = partitionState.getOrElseUpdate(topicAndPartition, NonExistentPartition)
    try {
      targetState match {
        case NewPartition =>
          // pre: partition did not exist before this
          assertValidPreviousStates(topicAndPartition, List(NonExistentPartition), NewPartition)
          assignReplicasToPartitions(topic, partition)
          partitionState.put(topicAndPartition, NewPartition)
          val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).mkString(",")
          trace("Controller %d epoch %d changed partition %s state from NotExists to New with assigned replicas %s"
            .format(controllerId, controller.epoch, topicAndPartition, assignedReplicas))
        // post: partition has been assigned replicas
      }
    } catch {
      case t: Throwable =>
        error("Controller %d epoch %d initiated state change for partition %s from %s to %s failed"
          .format(controllerId, controller.epoch, topicAndPartition, currState, targetState), t)
    }
  }


  /**
   * This API is invoked by the partition change zookeeper listener
 *
   * @param partitions   The list of partitions that need to be transitioned to the target state
   * @param targetState  The state that the partitions should be moved to
   */
  def handleStateChanges(partitions: Set[TopicAndPartition], targetState: PartitionState,
                         leaderSelector: PartitionLeaderSelector = noOpPartitionLeaderSelector) {
    info("Invoking state change to %s for partitions %s".format(targetState, partitions.mkString(",")))
    try {
      brokerRequestBatch.newBatch()
      partitions.foreach { topicAndPartition =>
        handleStateChange(topicAndPartition.topic, topicAndPartition.partition, targetState, leaderSelector)
      }
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch, controllerContext.correlationId.getAndIncrement)
    }catch {
      case e: Throwable => error("Error while moving some partitions to %s state".format(targetState), e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger state changes for those partitions
    }
  }

  class AddPartitionsListener(topic: String) extends IZkDataListener with Logging {

    this.logIdent = "[AddPartitionsListener on " + controller.config.brokerId + "]: "

    @throws(classOf[Exception])
    def handleDataChange(dataPath : String, data: Object) {
      controllerContext.controllerLock synchronized {
        try {
          info("Add Partition triggered " + data.toString + " for path " + dataPath)
          val partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, List(topic))
          val partitionsRemainingToBeAdded = partitionReplicaAssignment.filter(p =>
            !controllerContext.partitionReplicaAssignment.contains(p._1))
          info("New partitions to be added [%s]".format(partitionsRemainingToBeAdded))
          controller.onNewPartitionCreation(partitionsRemainingToBeAdded.keySet.toSet)
        } catch {
          case e: Throwable => error("Error while handling add partitions for data path " + dataPath, e )
        }
      }
    }

    @throws(classOf[Exception])
    def handleDataDeleted(parentPath : String) {
      // this is not implemented for partition change
    }
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a partition
   */
  class TopicChangeListener extends IZkChildListener with Logging {
    this.logIdent = "[TopicChangeListener on Controller " + controller.config.brokerId + "]: "

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, children : java.util.List[String]) {
      controllerContext.controllerLock synchronized {
        if (hasStarted.get) {
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
            if(newTopics.size > 0)
              controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet.toSet)
          } catch {
            case e: Throwable => error("Error while handling new topic", e )
          }
          // TODO: kafka-330  Handle deleted topics
        }
      }
    }
  }
}

