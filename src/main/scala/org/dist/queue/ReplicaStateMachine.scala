package org.dist.queue

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.utils.ZkUtils

import scala.collection.{Seq, Set, mutable}

class ReplicaStateMachine(controller: Controller) extends Logging {
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkClient = controllerContext.zkClient
  var replicaState: mutable.Map[(String, Int, Int), ReplicaState] = mutable.Map.empty
  val brokerRequestBatch = new ControllerBrokerRequestBatch(controller.controllerContext, controller.sendRequest,
    controllerId, controller.clientId)

  private val hasStarted = new AtomicBoolean(false)
  logIdent = "[Replica state machine on controller " + controller.config.brokerId + "]: "

  def startup() {
    // initialize replica state
    initializeReplicaState()
    hasStarted.set(true)
    // move all Online replicas to Online
    handleStateChanges(getAllReplicasOnBroker(controllerContext.allTopics.toSeq,
      controllerContext.liveBrokerIds.toSeq), OnlineReplica)
    info("Started replica state machine with initial state -> " + replicaState.toString())
  }


  private def getAllReplicasOnBroker(topics: Seq[String], brokerIds: Seq[Int]): Set[PartitionAndReplica] = {
    brokerIds.map { brokerId =>
      val partitionsAssignedToThisBroker: mutable.Map[TopicAndPartition, Seq[Int]] =
        controllerContext.partitionReplicaAssignment.filter(p => topics.contains(p._1.topic) && p._2.contains(brokerId))
      if(partitionsAssignedToThisBroker.size == 0)
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

   import scala.collection.JavaConverters._
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
//        newBrokers.foreach(controllerContext.controllerChannelManager.addBroker(_))
//        deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker(_))
        if(newBrokerIds.size > 0)
          controller.onBrokerStartup(newBrokerIds.toSeq)
        if(deadBrokerIds.size > 0)
          controller.onBrokerFailure(deadBrokerIds.toSeq)
      } catch {
        case e: Throwable => error("Error while handling broker changes", e)
      }
    }
  }

  def handleStateChange(topic: String, partition: Int, replica: Int, targetState: ReplicaState) = {
    info(s"Handling state changes for ${topic} partitions ${partition} replica ${replica} targetState ${targetState}")
  }

  /**
   * This API is invoked by the broker change controller callbacks and the startup API of the state machine
 *
   * @param replicas     The list of replicas (brokers) that need to be transitioned to the target state
   * @param targetState  The state that the replicas should be moved to
   * The controller's allLeaders cache should have been updated before this
   */
  def handleStateChanges(replicas: Set[PartitionAndReplica], targetState: ReplicaState) {
    info("Invoking state change to %s for replicas %s".format(targetState, replicas.mkString(",")))
    try {
      brokerRequestBatch.newBatch()
      replicas.foreach(r => handleStateChange(r.topic, r.partition, r.replica, targetState))
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch, controllerContext.correlationId.getAndIncrement)
    }catch {
      case e: Throwable => error("Error while moving some replicas to %s state".format(targetState), e)
    }
  }
}
