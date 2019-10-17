package org.dist.queue.controller

import java.util.concurrent.atomic.AtomicBoolean

import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.dist.queue.common.{Logging, StateChangeFailedException, TopicAndPartition}
import org.dist.queue.utils.ZkUtils

import scala.collection.{Seq, Set, mutable}

class PartitionStateMachine(controller: Controller) extends Logging {
  val brokerRequestBatch = new ControllerBrokerRequestBatch(controller.controllerContext, controller.sendRequest,
    controllerId, controller.clientId)

  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkClient = controllerContext.zkClient
  private val hasStarted = new AtomicBoolean(false)
  private val noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext)
  var partitionState: mutable.Map[TopicAndPartition, PartitionState] = mutable.Map.empty

  def registerPartitionChangeListener(topic: String) = {
    zkClient.subscribeDataChanges(ZkUtils.getTopicPath(topic), new AddPartitionsListener(topic, controller))
  }

  def startup() = {
    // initialize partition state
    initializePartitionState()
    hasStarted.set(true)
    // try to move partitions to online state
    triggerOnlinePartitionStateChange()
    info("Started partition state machine with initial state -> " + partitionState.toString())
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper
   */
  private def initializePartitionState() {
    for((topicPartition, replicaAssignment) <- controllerContext.partitionReplicaAssignment) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      controllerContext.partitionLeadershipInfo.get(topicPartition) match {
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          controllerContext.liveBrokerIds.contains(currentLeaderIsrAndEpoch.leaderAndIsr.leader) match {
            case true => // leader is alive
              partitionState.put(topicPartition, OnlinePartition)
            case false =>
              partitionState.put(topicPartition, OfflinePartition)
          }
        case None =>
          partitionState.put(topicPartition, NewPartition)
      }
    }
  }

  def registerListeners() = {
    registerTopicChangeListener()
  }


  private def registerTopicChangeListener() = {
    zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, new TopicChangeListener(controller, hasStarted))
  }

  /**
   * This API is invoked by the partition change zookeeper listener
   *
   * @param partitions  The list of partitions that need to be transitioned to the target state
   * @param targetState The state that the partitions should be moved to
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
    } catch {
      case e: Throwable => error("Error while moving some partitions to %s state".format(targetState), e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger state changes for those partitions
    }
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
        case OnlinePartition =>
          assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OnlinePartition)
          partitionState(topicAndPartition) match {
            case NewPartition =>
              // initialize leader and isr path for new partition
              initializeLeaderAndIsrForPartition(topicAndPartition)
            case OfflinePartition =>
              electLeaderForPartition(topic, partition, leaderSelector)
            case OnlinePartition => // invoked when the leader needs to be re-elected
              electLeaderForPartition(topic, partition, leaderSelector)
            case _ => // should never come here since illegal previous states are checked above
          }
      }
    } catch {
      case t: Throwable =>
        error("Controller %d epoch %d initiated state change for partition %s from %s to %s failed"
          .format(controllerId, controller.epoch, topicAndPartition, currState, targetState), t)
    }
  }

  def initializeLeaderAndIsrForPartition(topicAndPartition: TopicAndPartition) = {
    val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
    val liveAssignedReplicas = replicaAssignment.filter(r => controllerContext.liveBrokerIds.contains(r))
    if (liveAssignedReplicas.size == 0) {
      val failMsg = ("encountered error during state change of partition %s from New to Online, assigned replicas are [%s], " +
        "live brokers are [%s]. No assigned replica is alive.")
        .format(topicAndPartition, replicaAssignment.mkString(","), controllerContext.liveBrokerIds)
      error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
      throw new StateChangeFailedException(failMsg)
    }
    debug("Live assigned replicas for partition %s are: [%s]".format(topicAndPartition, liveAssignedReplicas))
    // make the first replica in the list of assigned replicas, the leader
    val leader = liveAssignedReplicas.head
    val leaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader, liveAssignedReplicas.toList),
      controller.epoch)
    debug("Initializing leader and isr for partition %s to %s".format(topicAndPartition, leaderIsrAndControllerEpoch))
    try {
      ZkUtils.createPersistentPath(controllerContext.zkClient,
        ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
        ZkUtils.leaderAndIsrZkData(leaderIsrAndControllerEpoch.leaderAndIsr, controller.epoch))
      // NOTE: the above write can fail only if the current controller lost its zk session and the new controller
      // took over and initialized this partition. This can happen if the current controller went into a long
      // GC pause
      controllerContext.partitionLeadershipInfo.put(topicAndPartition, leaderIsrAndControllerEpoch)
      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(liveAssignedReplicas, topicAndPartition.topic,
        topicAndPartition.partition, leaderIsrAndControllerEpoch, replicaAssignment)
    } catch {
      case e: ZkNodeExistsException =>
        // read the controller epoch
        val leaderIsrAndEpoch = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topicAndPartition.topic,
          topicAndPartition.partition).get
        val failMsg = ("encountered error while changing partition %s's state from New to Online since LeaderAndIsr path already " +
          "exists with value %s and controller epoch %d")
          .format(topicAndPartition, leaderIsrAndEpoch.leaderAndIsr.toString(), leaderIsrAndEpoch.controllerEpoch)
        error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
        throw new StateChangeFailedException(failMsg)
    }
  }

  def electLeaderForPartition(topic: String, partition: Int, leaderSelector: PartitionLeaderSelector) = {

  }

  private def assertValidPreviousStates(topicAndPartition: TopicAndPartition, fromStates: Seq[PartitionState],
                                        targetState: PartitionState) {
    if (!fromStates.contains(partitionState(topicAndPartition)))
      throw new IllegalStateException("Partition %s should be in the %s states before moving to %s state"
        .format(topicAndPartition, fromStates.mkString(","), targetState) + ". Instead it is in %s state"
        .format(partitionState(topicAndPartition)))
  }

  def assignReplicasToPartitions(topic: String, partition: Int) = {
    val assignedReplicas = ZkUtils.getReplicasForPartition(controllerContext.zkClient, topic, partition)
    controllerContext.partitionReplicaAssignment += TopicAndPartition(topic, partition) -> assignedReplicas
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
   */
  def triggerOnlinePartitionStateChange() {
    try {
      brokerRequestBatch.newBatch()
      // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state
      for ((topicAndPartition, partitionState) <- partitionState) {
        if (partitionState.equals(OfflinePartition) || partitionState.equals(NewPartition))
          handleStateChange(topicAndPartition.topic, topicAndPartition.partition, OnlinePartition, controller.offlinePartitionSelector)
      }
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch, controllerContext.correlationId.getAndIncrement)
    } catch {
      case e: Throwable => error("Error while moving some partitions to the online state", e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger leader election for those partitions
    }
  }

  def shutdown() = {
    hasStarted.set(false)
    partitionState.clear()
  }
}


sealed trait PartitionState {
  def state: Byte
}

case object NewPartition extends PartitionState {
  val state: Byte = 0
}

case object OnlinePartition extends PartitionState {
  val state: Byte = 1
}

case object OfflinePartition extends PartitionState {
  val state: Byte = 2
}

case object NonExistentPartition extends PartitionState {
  val state: Byte = 3
}


