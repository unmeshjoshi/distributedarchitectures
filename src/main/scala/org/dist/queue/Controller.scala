package org.dist.queue

import java.util.concurrent.atomic.AtomicInteger

import org.I0Itec.zkclient.ZkClient
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

class Controller(config:Config, zkClient:ZkClient) extends Logging {
  val controllerContext = new ControllerContext(zkClient, config.zkSessionTimeoutMs)
  val elector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath, onControllerFailover,
    config.brokerId)

  def startUp() = {
    elector.startup()
  }

  val isRunning: Boolean = true

  val partitionStateMachine = new PartitionStateMachine(this)

  def sendUpdateMetadataRequest(toSeq: scala.Seq[Int]): Unit = ???


  def epoch = controllerContext.epoch

  def onControllerFailover() {
    if(isRunning) {
      info("Broker %d starting become controller state transition".format(config.brokerId))
      partitionStateMachine.registerListeners()
      initializeControllerContext()
      partitionStateMachine.startup()
      info("Broker %d is ready to serve as the new controller with epoch %d".format(config.brokerId, epoch))
      /* send partition leadership info to all live brokers */
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
    }
    else
      info("Controller has been shut down, aborting startup/failover")
  }

  def updateLeaderAndIsrCache() = ???

  def startChannelManager() = ???

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

}
