package org.dist.queue

import java.util.concurrent.atomic.AtomicBoolean

import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener}
import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{LeaderAndIsrRequest, RequestKeys, RequestOrResponse, UpdateMetadataRequest}
import org.dist.queue.utils.ZkUtils

import scala.collection.JavaConverters._
import scala.collection.{Seq, Set, mutable}

class ControllerBrokerRequestBatch(controllerContext: ControllerContext, sendRequest: (Int, RequestOrResponse, (RequestOrResponse) => Unit) => Unit,
                                   controllerId: Int, clientId: String) extends Logging {
  val leaderAndIsrRequestMap = new mutable.HashMap[Int, mutable.HashMap[(String, Int), PartitionStateInfo]]
  val stopReplicaRequestMap = new mutable.HashMap[Int, Seq[(String, Int)]]
  val stopAndDeleteReplicaRequestMap = new mutable.HashMap[Int, Seq[(String, Int)]]
  val updateMetadataRequestMap = new mutable.HashMap[Int, mutable.HashMap[TopicAndPartition, PartitionStateInfo]]

  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicas: Seq[Int]) = {
    brokerIds.foreach { brokerId =>
      leaderAndIsrRequestMap.getOrElseUpdate(brokerId, new mutable.HashMap[(String, Int), PartitionStateInfo])
      leaderAndIsrRequestMap(brokerId).put((topic, partition),
        PartitionStateInfo(leaderIsrAndControllerEpoch, replicas.toSet))
    }
    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(TopicAndPartition(topic, partition)))
  }

  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: scala.collection.Set[TopicAndPartition] = Set.empty[TopicAndPartition]) {
    val partitionList =
      if (partitions.isEmpty) {
        controllerContext.partitionLeadershipInfo.keySet
      } else {
        partitions
      }
    partitionList.foreach { partition =>
      val leaderIsrAndControllerEpochOpt = controllerContext.partitionLeadershipInfo.get(partition)
      leaderIsrAndControllerEpochOpt match {
        case Some(leaderIsrAndControllerEpoch) =>
          val replicas = controllerContext.partitionReplicaAssignment(partition).toSet
          val partitionStateInfo = PartitionStateInfo(leaderIsrAndControllerEpoch, replicas)
          brokerIds.foreach { brokerId =>
            updateMetadataRequestMap.getOrElseUpdate(brokerId, new mutable.HashMap[TopicAndPartition, PartitionStateInfo])
            updateMetadataRequestMap(brokerId).put(partition, partitionStateInfo)
          }
        case None =>
          info("Leader not assigned yet for partition %s. Skip sending udpate metadata request".format(partition))
      }
    }
  }


  def sendRequestsToBrokers(controllerEpoch: Int, correlationId: Int): Unit = {
    //send leaderandisr requests
    //send update metadata requests
    //send stop replica requests
    leaderAndIsrRequestMap.foreach { m =>
      val broker = m._1
      val partitionStateInfos: Map[(String, Int), PartitionStateInfo] = m._2.toMap
      val func = (tuple: ((String, Int), PartitionStateInfo)) ⇒ tuple._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader
      val leaderIds = partitionStateInfos.map(func).toSet
      val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id))
      val leaderAndIsrRequest = LeaderAndIsrRequest(partitionStateInfos,
        leaders, controllerId, controllerEpoch, correlationId, clientId)
      for (p <- partitionStateInfos) {
        val typeOfRequest = if (broker == p._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader) "become-leader" else "become-follower"
        trace(("Controller %d epoch %d sending %s LeaderAndIsr request with correlationId %d to broker %d " +
          "for partition [%s,%d]").format(controllerId, controllerEpoch, typeOfRequest, correlationId, broker,
          p._1._1, p._1._2))
      }
      sendRequest(broker, RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndIsrRequest), 1), null)
    }
    leaderAndIsrRequestMap.clear()
    updateMetadataRequestMap.foreach { m =>
      val broker = m._1
      val partitionStateInfos = m._2.toMap
      val updateMetadataRequest = UpdateMetadataRequest(controllerId, controllerEpoch, correlationId, clientId,
        partitionStateInfos, controllerContext.liveOrShuttingDownBrokers)
      partitionStateInfos.foreach(p => trace(("Controller %d epoch %d sending UpdateMetadata request with " +
        "correlationId %d to broker %d for partition %s").format(controllerId, controllerEpoch, correlationId, broker, p._1)))
      sendRequest(broker, RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest),1), null)
    }
    updateMetadataRequestMap.clear()
  }

  def newBatch() = {
    // raise error if the previous batch is not empty
    if (leaderAndIsrRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        "a new one. Some LeaderAndIsr state changes %s might be lost ".format(leaderAndIsrRequestMap.toString()))
    if (stopReplicaRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some StopReplica state changes %s might be lost ".format(stopReplicaRequestMap.toString()))
    if (updateMetadataRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some UpdateMetadata state changes %s might be lost ".format(updateMetadataRequestMap.toString()))
    if (stopAndDeleteReplicaRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some StopReplica with delete state changes %s might be lost ".format(stopAndDeleteReplicaRequestMap.toString()))

    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    updateMetadataRequestMap.clear()
    stopAndDeleteReplicaRequestMap.clear()
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

case class PartitionStateMachine(controller: Controller) extends Logging {

  val brokerRequestBatch = new ControllerBrokerRequestBatch(controller.controllerContext, controller.sendRequest,
    controllerId, controller.clientId)
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkClient = controllerContext.zkClient
  private val hasStarted = new AtomicBoolean(false)
  private val noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext)
  var partitionState: mutable.Map[TopicAndPartition, PartitionState] = mutable.Map.empty

  def registerPartitionChangeListener(topic: String) = {
    zkClient.subscribeDataChanges(ZkUtils.getTopicPath(topic), new AddPartitionsListener(topic))
  }

  def startup() = {
    hasStarted.set(true)
  }


  def registerListeners() = {
    registerTopicChangeListener()
  }


  private def registerTopicChangeListener() = {
    zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, new TopicChangeListener())
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

  def electLeaderForPartition(topic: String, partition: Int, leaderSelector: PartitionLeaderSelector) = ???

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

  class AddPartitionsListener(topic: String) extends IZkDataListener with Logging {

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

  /**
   * This is the zookeeper listener that triggers all the state transitions for a partition
   */
  class TopicChangeListener extends IZkChildListener with Logging {
    this.logIdent = "[TopicChangeListener on Controller " + controller.config.brokerId + "]: "

    @throws(classOf[Exception])
    def handleChildChange(parentPath: String, children: java.util.List[String]) {
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

}

