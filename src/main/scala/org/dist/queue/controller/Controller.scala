package org.dist.queue.controller

import java.util.concurrent.atomic.AtomicInteger

import org.I0Itec.zkclient.{IZkDataListener, ZkClient}
import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.RequestOrResponse
import org.dist.queue.common.{Logging, TopicAndPartition}
import org.dist.queue.network.SocketServer
import org.dist.queue.server.{Config, ZookeeperLeaderElector}
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.utils.{Utils, ZkUtils}

import scala.collection.immutable.Set
import scala.collection.{Map, Seq, mutable}

object Controller extends Logging {
  val InitialControllerEpoch = 1
  val InitialControllerEpochZkVersion = 1

  def parseControllerId(controllerInfoString: String): Int = {
    try {
      val controllerInfo = JsonSerDes.deserialize(controllerInfoString.getBytes, classOf[Map[String, Int]])
      controllerInfo("brokerid")
    } catch {
      case t: Throwable =>
          throw new RuntimeException("Failed to parse the controller info: " + controllerInfoString, t)
    }
  }
}


case class PartitionStateInfo(val leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                              val allReplicas: Set[Int]) {
  def replicationFactor = allReplicas.size
}

object LeaderAndIsr {
  val initialLeaderEpoch: Int = 0
  val initialZKVersion: Int = 0
}

case class LeaderAndIsr(var leader: Int, var leaderEpoch: Int, var isr: List[Int], var zkVersion: Int) {
  def this(leader: Int, isr: List[Int]) = this(leader, LeaderAndIsr.initialLeaderEpoch, isr, LeaderAndIsr.initialZKVersion)

  override def toString(): String = {
    val jsonDataMap = new collection.mutable.HashMap[String, String]
    jsonDataMap.put("leader", leader.toString)
    jsonDataMap.put("leaderEpoch", leaderEpoch.toString)
    jsonDataMap.put("ISR", isr.mkString(","))
    Utils.mapToJson(jsonDataMap, valueInQuotes = true)
  }
}


class ReassignedPartitionsIsrChangeListener(controller: Controller, topic: String, partition: Int,
                                            reassignedReplicas: Set[Int])
  extends IZkDataListener with Logging {
  this.logIdent = "[ReassignedPartitionsIsrChangeListener on controller " + controller.config.brokerId + "]: "
  val zkClient = controller.controllerContext.zkClient
  val controllerContext = controller.controllerContext

  /**
   * Invoked when some partitions need to move leader to preferred replica
   * @throws Exception On any error.
   */
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    try {
      controllerContext.controllerLock synchronized {
        debug("Reassigned partitions isr change listener fired for path %s with children %s".format(dataPath, data))
        // check if this partition is still being reassigned or not
        val topicAndPartition = TopicAndPartition(topic, partition)
      }

    }catch {
      case e: Throwable => error("Error while handling partition reassignment", e)
    }
  }

  /**
   * @throws Exception
   *             On any error.
   */
  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
  }
}

case class ReassignedPartitionsContext(var newReplicas: Seq[Int] = Seq.empty,
                                       var isrChangeListener: ReassignedPartitionsIsrChangeListener = null)

case class PartitionAndReplica(topic: String, partition: Int, replica: Int)

case class LeaderIsrAndControllerEpoch(val leaderAndIsr: LeaderAndIsr, controllerEpoch: Int)

class ControllerContext(val zkClient:ZkClient, val zkSessionTimeoutMs: Int = 6000,
                        var controllerChannelManager: ControllerChannelManager = null,
                        var epoch: Int = Controller.InitialControllerEpoch - 1,
                        var epochZkVersion: Int = Controller.InitialControllerEpochZkVersion - 1,
                        val controllerLock: Object = new Object,
                        val correlationId: AtomicInteger = new AtomicInteger(0),
                        var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty,
                        var allTopics: Set[String] = Set.empty,
                        var partitionReplicaAssignment: mutable.Map[TopicAndPartition, Seq[Int]] = mutable.Map.empty,
                        var partitionLeadershipInfo: mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = mutable.Map.empty,
                        var partitionsBeingReassigned: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] =
                        new mutable.HashMap,
                        var partitionsUndergoingPreferredReplicaElection: mutable.Set[TopicAndPartition] =
                        new mutable.HashSet) {



  private var liveBrokersUnderlying: Set[Broker] = Set.empty
  private var liveBrokerIdsUnderlying: Set[Int] = Set.empty

  // setter
  def liveBrokers_=(brokers: Set[Broker]) {
    liveBrokersUnderlying = brokers
    liveBrokerIdsUnderlying = liveBrokersUnderlying.map(_.id)
  }

  // getter
  def liveBrokers = liveBrokersUnderlying.filter(broker => !shuttingDownBrokerIds.contains(broker.id))
  def liveBrokerIds = liveBrokerIdsUnderlying.filter(brokerId => !shuttingDownBrokerIds.contains(brokerId))

  def liveOrShuttingDownBrokerIds = liveBrokerIdsUnderlying
  def liveOrShuttingDownBrokers = liveBrokersUnderlying
}

class Controller(val config:Config, val zkClient:ZkClient, val socketServer:SocketServer) extends Logging {

  def clientId = "id_%d-host_%s-port_%d".format(config.brokerId, config.hostName, config.port)


  val controllerContext = new ControllerContext(zkClient, config.zkSessionTimeoutMs)
  val elector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath, onControllerFailover,
    config.brokerId)
  private val replicaStateMachine = new ReplicaStateMachine(this)
  val brokerRequestBatch = new ControllerBrokerRequestBatch(controllerContext, sendRequest, this.config.brokerId, this.clientId)
  registerControllerChangedListener()


  def startup() = {
    elector.startup()
  }

  var isRunning: Boolean = true

  val partitionStateMachine = new PartitionStateMachine(this)

  private def registerControllerChangedListener() {
    zkClient.subscribeDataChanges(ZkUtils.ControllerEpochPath, new ControllerEpochListener(this))
  }


  class ControllerEpochListener(controller: Controller) extends IZkDataListener with Logging {
    this.logIdent = "[ControllerEpochListener on " + controller.config.brokerId + "]: "
    val controllerContext = controller.controllerContext
    readControllerEpochFromZookeeper()

    /**
     * Invoked when a controller updates the epoch value
     * @throws Exception On any error.
     */
    @throws(classOf[Exception])
    def handleDataChange(dataPath: String, data: Object) {
      debug("Controller epoch listener fired with new epoch " + data.toString)
      controllerContext.controllerLock synchronized {
        // read the epoch path to get the zk version
        readControllerEpochFromZookeeper()
      }
    }

    /**
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
    }

    private def readControllerEpochFromZookeeper() {
      // initialize the controller epoch and zk version by reading from zookeeper
      if(ZkUtils.pathExists(controllerContext.zkClient, ZkUtils.ControllerEpochPath)) {
        val epochData = ZkUtils.readData(controllerContext.zkClient, ZkUtils.ControllerEpochPath)
        controllerContext.epoch = epochData._1.toInt
        controllerContext.epochZkVersion = epochData._2.getVersion
        info("Initialized controller epoch to %d and zk version %d".format(controllerContext.epoch, controllerContext.epochZkVersion))
      }
    }
  }


  def sendUpdateMetadataRequest(brokers: scala.Seq[Int], partitions: Set[TopicAndPartition] = Set.empty[TopicAndPartition]): Unit = {
    info(s"Update metadata request ${brokers}")
    brokerRequestBatch.newBatch()
    brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
    brokerRequestBatch.sendRequestsToBrokers(epoch, controllerContext.correlationId.getAndIncrement)
  }


  def epoch = controllerContext.epoch

  def onControllerFailover() {
    if(isRunning) {
      info("Broker %d starting become controller state transition".format(config.brokerId))
      partitionStateMachine.registerListeners()
      replicaStateMachine.registerListeners()
      initializeControllerContext()
      replicaStateMachine.startup()
      partitionStateMachine.startup()
      info("Broker %d is ready to serve as the new controller with epoch %d".format(config.brokerId, epoch))
      /* send partition leadership info to all live brokers */
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
    }
    else
      info("Controller has been shut down, aborting startup/failover")
  }

  private def updateLeaderAndIsrCache() {
    val leaderAndIsrInfo: mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = ZkUtils.getPartitionLeaderAndIsrForTopics(zkClient, controllerContext.partitionReplicaAssignment.keySet)
    for((topicPartition, leaderIsrAndControllerEpoch) <- leaderAndIsrInfo)
      controllerContext.partitionLeadershipInfo.put(topicPartition, leaderIsrAndControllerEpoch)
  }

  def startChannelManager() = {
    info("Starting channel manager")
    controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config, socketServer)
    controllerContext.controllerChannelManager.startup()
  }

  private def initializeControllerContext() {
    controllerContext.liveBrokers = ZkUtils.getAllBrokersInCluster(zkClient).toSet
    controllerContext.allTopics = ZkUtils.getAllTopics(zkClient).toSet
    controllerContext.partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, controllerContext.allTopics.toSeq)
    controllerContext.partitionLeadershipInfo = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
    controllerContext.shuttingDownBrokerIds = mutable.Set.empty[Int]
    // update the leader and isr cache for all existing partitions from Zookeeper
    updateLeaderAndIsrCache()
    // start the channel manager
    startChannelManager()
    info("Currently active brokers in the cluster: %s".format(controllerContext.liveBrokerIds))
    info("Currently shutting brokers in the cluster: %s".format(controllerContext.shuttingDownBrokerIds))
    info("Current list of topics in the cluster: %s".format(controllerContext.allTopics))
  }

  /**
   * This callback is invoked by the partition state machine's topic change listener with the list of failed brokers
   * as input. It does the following -
   * 1. Registers partition change listener. This is not required until KAFKA-347
   * 2. Invokes the new partition callback
   */
  def onNewTopicCreation(topics: Set[String], newPartitions: Set[TopicAndPartition]) {
    info("New topic creation callback for %s".format(newPartitions.mkString(",")))
    // subscribe to partition changes
    topics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
    onNewPartitionCreation(newPartitions)
  }

  /**
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
   */
  def onNewPartitionCreation(newPartitions: Set[TopicAndPartition]) {
    info("New partition creation callback for %s".format(newPartitions.mkString(",")))
    partitionStateMachine.handleStateChanges(newPartitions, NewPartition)
    replicaStateMachine.handleStateChanges(getAllReplicasForPartition(newPartitions), NewReplica)
    partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition, offlinePartitionSelector)
    replicaStateMachine.handleStateChanges(getAllReplicasForPartition(newPartitions), OnlineReplica)
  }

  def onPartitionReassignment(topicAndPartition: TopicAndPartition, reassignedPartitionContext: ReassignedPartitionsContext): Unit = {
    info("on partition reassignment")
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
   * brokers as input. It does the following -
   * 1. Triggers the OnlinePartition state change for all new/offline partitions
   * 2. It checks whether there are reassigned replicas assigned to any newly started brokers.  If
   *    so, it performs the reassignment logic for each topic/partition.
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
   * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
   *    partitions currently new or offline (rather than every partition this controller is aware of)
   * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
   *    every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
   */
  def onBrokerStartup(newBrokers: Seq[Int]) {
    info("New broker startup callback for %s".format(newBrokers.mkString(",")))

    val newBrokersSet = newBrokers.toSet
    // send update metadata request for all partitions to the newly restarted brokers. In cases of controlled shutdown
    // leaders will not be elected when a new broker comes up. So at least in the common controlled shutdown case, the
    // metadata will reach the new brokers faster
    sendUpdateMetadataRequest(newBrokers.toSeq)
    // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
    // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
    replicaStateMachine.handleStateChanges(getAllReplicasOnBroker(zkClient, controllerContext.allTopics.toSeq, newBrokers), OnlineReplica)
    // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
    // to see if these brokers can become leaders for some/all of those
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // check if reassignment of some partitions need to be restarted
    val partitionsWithReplicasOnNewBrokers = controllerContext.partitionsBeingReassigned.filter{
      case (topicAndPartition, reassignmentContext) =>
        reassignmentContext.newReplicas.exists(newBrokersSet.contains(_))
    }
    partitionsWithReplicasOnNewBrokers.foreach(p => onPartitionReassignment(p._1, p._2))
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
   * as input. It does the following -
   * 1. Mark partitions with dead leaders as offline
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. Invokes the OfflineReplica state change on the input list of newly started brokers
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point.  This is because
   * the partition state machine will refresh our cache for us when performing leader election for all new/offline
   * partitions coming online.
   */
  def onBrokerFailure(deadBrokers: Seq[Int]) {
    info("Broker failure callback for %s".format(deadBrokers.mkString(",")))

    val deadBrokersThatWereShuttingDown =
      deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
    info("Removed %s from list of shutting down brokers.".format(deadBrokersThatWereShuttingDown))

    val deadBrokersSet = deadBrokers.toSet
    // trigger OfflinePartition state for all partitions whose current leader is one amongst the dead brokers
    val partitionsWithoutLeader = controllerContext.partitionLeadershipInfo.filter(partitionAndLeader =>
      deadBrokersSet.contains(partitionAndLeader._2.leaderAndIsr.leader)).keySet
    partitionStateMachine.handleStateChanges(partitionsWithoutLeader, OfflinePartition)
    // trigger OnlinePartition state changes for offline or new partitions
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // handle dead replicas
    replicaStateMachine.handleStateChanges(getAllReplicasOnBroker(zkClient, controllerContext.allTopics.toSeq, deadBrokers), OfflineReplica)
  }


  def getAllReplicasOnBroker(zkClient: ZkClient, topics: Seq[String], brokerIds: Seq[Int]): Set[PartitionAndReplica] = {
    Set.empty[PartitionAndReplica] ++ brokerIds.map { brokerId =>
      // read all the partitions and their assigned replicas into a map organized by
      // { replica id -> partition 1, partition 2...
      val partitionsAssignedToThisBroker = getPartitionsAssignedToBroker(zkClient, topics, brokerId)
      if(partitionsAssignedToThisBroker.size == 0)
        info("No state transitions triggered since no partitions are assigned to brokers %s".format(brokerIds.mkString(",")))
      partitionsAssignedToThisBroker.map(p => new PartitionAndReplica(p._1, p._2, brokerId))
    }.flatten
  }

  def getPartitionsAssignedToBroker(zkClient: ZkClient, topics: Seq[String], brokerId: Int): Seq[(String, Int)] = {
    val topicsAndPartitions: mutable.Map[String, Map[Int, Seq[Int]]] = ZkUtils.getPartitionAssignmentForTopics(zkClient, topics)
    topicsAndPartitions.map { topicAndPartitionMap:(String, Map[Int, Seq[Int]]) =>
      val topic = topicAndPartitionMap._1
      val partitionMap = topicAndPartitionMap._2
      val relevantPartitionsMap = partitionMap.filter( m => m._2.contains(brokerId) )
      val relevantPartitions = relevantPartitionsMap.map(_._1)
      for(relevantPartition <- relevantPartitions) yield {
        (topic, relevantPartition)
      }
    }.flatten[(String, Int)].toSeq
  }

  private def getAllReplicasForPartition(partitions: Set[TopicAndPartition]): Set[PartitionAndReplica] = {
    partitions.map { p =>
      val replicas = controllerContext.partitionReplicaAssignment(p)
      replicas.map(r => new PartitionAndReplica(p.topic, p.partition, r))
    }.flatten
  }

  def sendRequest(brokerId : Int, request : RequestOrResponse, callback: (RequestOrResponse) => Unit = null) = {
    controllerContext.controllerChannelManager.sendRequest(brokerId, request, callback)

  }

  val offlinePartitionSelector = new OfflinePartitionLeaderSelector(controllerContext)


  def shutdown() = {
    controllerContext.controllerLock synchronized {
      isRunning = false
      partitionStateMachine.shutdown()
      replicaStateMachine.shutdown()
      if(controllerContext.controllerChannelManager != null) {
        controllerContext.controllerChannelManager.shutdown()
        controllerContext.controllerChannelManager = null
        info("Controller shutdown complete")
      }
    }
  }

}


sealed trait ReplicaState { def state: Byte }
case object NewReplica extends ReplicaState { val state: Byte = 1 }
case object OnlineReplica extends ReplicaState { val state: Byte = 2 }
case object OfflineReplica extends ReplicaState { val state: Byte = 3 }
case object NonExistentReplica extends ReplicaState { val state: Byte = 4 }

