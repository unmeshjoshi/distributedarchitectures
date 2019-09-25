package org.dist.queue

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import org.I0Itec.zkclient.{IZkDataListener, ZkClient}
import org.dist.kvstore.JsonSerDes
import org.dist.queue.utils.ZkUtils
import org.dist.queue.utils.ZkUtils.Broker

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

  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(leaderIsrAndControllerEpoch.controllerEpoch)
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.leader)
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch)
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.isr.size)
    leaderIsrAndControllerEpoch.leaderAndIsr.isr.foreach(buffer.putInt(_))
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion)
    buffer.putInt(replicationFactor)
    allReplicas.foreach(buffer.putInt(_))
  }

  def sizeInBytes(): Int = {
    val size =
      4 /* epoch of the controller that elected the leader */ +
        4 /* leader broker id */ +
        4 /* leader epoch */ +
        4 /* number of replicas in isr */ +
        4 * leaderIsrAndControllerEpoch.leaderAndIsr.isr.size /* replicas in isr */ +
        4 /* zk version */ +
        4 /* replication factor */ +
        allReplicas.size * 4
    size
  }

  override def toString(): String = {
    val partitionStateInfo = new StringBuilder
    partitionStateInfo.append("(LeaderAndIsrInfo:" + leaderIsrAndControllerEpoch.toString)
    partitionStateInfo.append(",ReplicationFactor:" + replicationFactor + ")")
    partitionStateInfo.append(",AllReplicas:" + allReplicas.mkString(",") + ")")
    partitionStateInfo.toString()
  }
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

case class LeaderIsrAndControllerEpoch(val leaderAndIsr: LeaderAndIsr, controllerEpoch: Int) {
  override def toString(): String = {
    val leaderAndIsrInfo = new StringBuilder
    leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader)
    leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr.mkString(","))
    leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch)
    leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")")
    leaderAndIsrInfo.toString()
  }
}

class ControllerContext(val zkClient:ZkClient, val zkSessionTimeoutMs: Int = 6000,
                        var epoch: Int = Controller.InitialControllerEpoch - 1,
                        var epochZkVersion: Int = Controller.InitialControllerEpochZkVersion - 1,
                        val controllerLock: Object = new Object,
                        val correlationId: AtomicInteger = new AtomicInteger(0),
                        var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty,
                        var allTopics: Set[String] = Set.empty,
                        var partitionReplicaAssignment: mutable.Map[TopicAndPartition, Seq[Int]] = mutable.Map.empty,
                        var partitionLeadershipInfo: mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = mutable.Map.empty) {

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

class Controller(val config:Config, val zkClient:ZkClient) extends Logging {
  def onBrokerFailure(toSeq: scala.Seq[Int]) = {

  }

  def onBrokerStartup(toSeq: scala.Seq[Int]) = {

  }


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

  val isRunning: Boolean = true

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
    val leaderAndIsrInfo = ZkUtils.getPartitionLeaderAndIsrForTopics(zkClient, controllerContext.partitionReplicaAssignment.keySet)
    for((topicPartition, leaderIsrAndControllerEpoch) <- leaderAndIsrInfo)
      controllerContext.partitionLeadershipInfo.put(topicPartition, leaderIsrAndControllerEpoch)
  }

  def startChannelManager() = {
    info("Starting channel manager")
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

  private def getAllReplicasForPartition(partitions: Set[TopicAndPartition]): Set[PartitionAndReplica] = {
    partitions.map { p =>
      val replicas = controllerContext.partitionReplicaAssignment(p)
      replicas.map(r => new PartitionAndReplica(p.topic, p.partition, r))
    }.flatten
  }

  def sendRequest(brokerId : Int, request : RequestOrResponse, callback: (RequestOrResponse) => Unit = null) = {

  }

  val offlinePartitionSelector = new OfflinePartitionLeaderSelector(controllerContext)

}


sealed trait ReplicaState { def state: Byte }
case object NewReplica extends ReplicaState { val state: Byte = 1 }
case object OnlineReplica extends ReplicaState { val state: Byte = 2 }
case object OfflineReplica extends ReplicaState { val state: Byte = 3 }
case object NonExistentReplica extends ReplicaState { val state: Byte = 4 }

