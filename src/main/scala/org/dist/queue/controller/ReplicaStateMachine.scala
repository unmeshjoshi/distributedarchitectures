package org.dist.queue.controller

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.common.{Logging, StateChangeFailedException, TopicAndPartition}
import org.dist.queue.utils.ZkUtils

import scala.collection.{Seq, Set, mutable}

class ReplicaStateMachine(controller: Controller) extends Logging {
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkClient = controllerContext.zkClient
  private val hasStarted = new AtomicBoolean(true)
  var replicaState: mutable.Map[(String, Int, Int), ReplicaState] = mutable.Map.empty
  logIdent = "[Replica state machine on controller " + controller.config.brokerId + "]: "

  val brokerRequestBatch = new ControllerBrokerRequestBatch(controller.controllerContext, controller.sendRequest, controllerId, controller.clientId)

  def startup() {
    // initialize replica state
    initializeReplicaState()
    hasStarted.set(true)
    // move all Online replicas to Online
    val allReplicasOnBroker = getAllReplicasOnBroker(controllerContext.allTopics.toSeq,
      controllerContext.liveBrokerIds.toSeq)
    handleStateChanges(allReplicasOnBroker, OnlineReplica)
    info("Started replica state machine with initial state -> " + replicaState.toString())
  }


  private def getAllReplicasOnBroker(topics: Seq[String], brokerIds: Seq[Int]): Set[PartitionAndReplica] = {
    brokerIds.map { brokerId =>
      val partitionsAssignedToThisBroker: mutable.Map[TopicAndPartition, Seq[Int]] =
      controllerContext.partitionReplicaAssignment.filter(p => topics.contains(p._1.topic) && p._2.contains(brokerId))
      if (partitionsAssignedToThisBroker.size == 0)
      info("No state transitions triggered since no partitions are assigned to brokers %s".format(brokerIds.mkString(",")))
      partitionsAssignedToThisBroker.toList.map(p => new PartitionAndReplica(p._1.topic, p._1.partition, brokerId))
    }.flatten.toSet
  }


  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
   */
  private def initializeReplicaState() {
    for ((topicPartition, assignedReplicas) <- controllerContext.partitionReplicaAssignment) {
      val topic = topicPartition.topic
      val partition = topicPartition.partition
      assignedReplicas.foreach { replicaId =>
        controllerContext.liveBrokerIds.contains(replicaId) match {
          case true => replicaState.put((topic, partition, replicaId), OnlineReplica)
          case false => replicaState.put((topic, partition, replicaId), OfflineReplica)
        }
      }
    }
  }

  /**
   * This API is invoked by the broker change controller callbacks and the startup API of the state machine
   *
   * @param replicas    The list of replicas (brokers) that need to be transitioned to the target state
   * @param targetState The state that the replicas should be moved to
   *                    The controller's allLeaders cache should have been updated before this
   */
  def handleStateChanges(replicas: Set[PartitionAndReplica], targetState: ReplicaState) {
    info("Invoking state change to %s for replicas %s".format(targetState, replicas.mkString(",")))
    try {
      brokerRequestBatch.newBatch()
      replicas.foreach(r => handleStateChange(r.topic, r.partition, r.replica, targetState))
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch, controllerContext.correlationId.getAndIncrement)
    } catch {
      case e: Throwable => error("Error while moving some replicas to %s state".format(targetState), e)
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state.
   *
   * @param topic       The topic of the replica for which the state transition is invoked
   * @param partition   The partition of the replica for which the state transition is invoked
   * @param replicaId   The replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   */
  def handleStateChange(topic: String, partition: Int, replicaId: Int, targetState: ReplicaState) {
    val topicAndPartition = TopicAndPartition(topic, partition)
    if (!hasStarted.get)
    throw new StateChangeFailedException(("Controller %d epoch %d initiated state change of replica %d for partition %s " +
    "to %s failed because replica state machine has not started")
    .format(controllerId, controller.epoch, replicaId, topicAndPartition, targetState))
    try {
      replicaState.getOrElseUpdate((topic, partition, replicaId), NonExistentReplica)
      val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
      targetState match {
        case NewReplica =>
          assertValidPreviousStates(topic, partition, replicaId, List(NonExistentReplica), targetState)
          // start replica as a follower to the current leader for its partition
          val leaderIsrAndControllerEpochOpt = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition)
          leaderIsrAndControllerEpochOpt match {
            case Some(leaderIsrAndControllerEpoch) =>
              if (leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
              throw new StateChangeFailedException("Replica %d for partition %s cannot be moved to NewReplica"
          .format(replicaId, topicAndPartition) + "state as it is being requested to become leader")
              brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId),
              topic, partition, leaderIsrAndControllerEpoch,
              replicaAssignment)
            case None => // new leader request will be sent to this replica when one gets elected
          }
          replicaState.put((topic, partition, replicaId), NewReplica)
          trace("Controller %d epoch %d changed state of replica %d for partition %s to NewReplica"
      .format(controllerId, controller.epoch, replicaId, topicAndPartition))
        case OnlineReplica =>
          assertValidPreviousStates(topic, partition, replicaId, List(NewReplica, OnlineReplica, OfflineReplica), targetState)
          replicaState((topic, partition, replicaId)) match {
            case NewReplica =>
              // add this replica to the assigned replicas list for its partition
              val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
              if (!currentAssignedReplicas.contains(replicaId))
              controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas :+ replicaId)
              trace("Controller %d epoch %d changed state of replica %d for partition %s to OnlineReplica"
          .format(controllerId, controller.epoch, replicaId, topicAndPartition))
            case _ =>
              // check if the leader for this partition ever existed
              controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
                case Some(leaderIsrAndControllerEpoch) =>
                  brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId), topic, partition, leaderIsrAndControllerEpoch,
                  replicaAssignment)
                  replicaState.put((topic, partition, replicaId), OnlineReplica)
                  trace("Controller %d epoch %d changed state of replica %d for partition %s to OnlineReplica"
              .format(controllerId, controller.epoch, replicaId, topicAndPartition))
                case None => // that means the partition was never in OnlinePartition state, this means the broker never
                // started a log for that partition and does not have a high watermark value for this partition
              }

          }
          replicaState.put((topic, partition, replicaId), OnlineReplica)
      }
    }
    catch {
      case t: Throwable =>
        error("Controller %d epoch %d initiated state change of replica %d for partition [%s,%d] to %s failed"
      .format(controllerId, controller.epoch, replicaId, topic, partition, targetState), t)
    }
  }


  private def assertValidPreviousStates(topic: String, partition: Int, replicaId: Int, fromStates: Seq[ReplicaState],
  targetState: ReplicaState) {
    assert(fromStates.contains(replicaState((topic, partition, replicaId))),
    "Replica %s for partition [%s,%d] should be in the %s states before moving to %s state"
    .format(replicaId, topic, partition, fromStates.mkString(","), targetState) +
    ". Instead it is in %s state".format(replicaState((topic, partition, replicaId))))
  }

  // register broker change listener
  def registerListeners() {
    registerBrokerChangeListener()
  }

  private def registerBrokerChangeListener() = {
    zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, new BrokerChangeListener())
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a replica
   */
  class BrokerChangeListener() extends IZkChildListener with Logging {
    this.logIdent = "[BrokerChangeListener on Controller " + controller.config.brokerId + "]: "

    import scala.jdk.CollectionConverters._

    override def handleChildChange(parentPath: String, currentBrokerList: util.List[String]): Unit = {
      info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.asScala.mkString(",")))
      try {
        val curBrokerIds = currentBrokerList.asScala.map(_.toInt).toSet
        val newBrokerIds = curBrokerIds -- controllerContext.liveOrShuttingDownBrokerIds
        val newBrokerInfo = newBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _))
        val newBrokers = newBrokerInfo.filter(_.isDefined).map(_.get)
        val deadBrokerIds = controllerContext.liveOrShuttingDownBrokerIds -- curBrokerIds
        controllerContext.liveBrokers = curBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
        info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
        .format(newBrokerIds.mkString(","), deadBrokerIds.mkString(","), controllerContext.liveBrokerIds.mkString(",")))

        newBrokers.foreach(controllerContext.controllerChannelManager.addBroker(_))
        deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker(_))

        if (newBrokerIds.size > 0)
        controller.onBrokerStartup(newBrokerIds.toSeq)
        if (deadBrokerIds.size > 0)
        controller.onBrokerFailure(deadBrokerIds.toSeq)
      } catch {
        case e: Throwable => error("Error while handling broker changes", e)
      }
    }
  }

  def shutdown() {
    hasStarted.set(false)
    replicaState.clear()
  }

}
