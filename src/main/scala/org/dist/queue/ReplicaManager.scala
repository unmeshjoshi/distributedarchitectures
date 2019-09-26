package org.dist.queue

import java.util.concurrent.atomic.AtomicBoolean

import org.I0Itec.zkclient.ZkClient
import org.dist.queue.api.LeaderAndIsrRequest
import org.dist.queue.utils.ZkUtils
import org.dist.queue.utils.ZkUtils.Broker

import scala.collection.{Set, mutable}


object ReplicaManager {
  val UnknownLogEndOffset = -1L
}

class ReplicaManager(val config: Config,
                     time: Time,
                     val zkClient: ZkClient,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean ) extends Logging {
  val replicaFetcherManager = new ReplicaFetcherManager

  var controllerEpoch: Int = Controller.InitialControllerEpoch - 1
  private val localBrokerId = config.brokerId
  private val allPartitions = new Pool[(String, Int), Partition]
  private var leaderPartitions = new mutable.HashSet[Partition]()
  private val leaderPartitionsLock = new Object

  private def makeLeader(controllerId: Int, epoch:Int, topic: String, partitionId: Int,
                         partitionStateInfo: PartitionStateInfo, correlationId: Int) = {
    val leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch
    trace(("Broker %d received LeaderAndIsr request correlationId %d from controller %d epoch %d " +
      "starting the become-leader transition for partition [%s,%d]")
      .format(localBrokerId, correlationId, controllerId, epoch, topic, partitionId))


    val partition = getOrCreatePartition(topic, partitionId, partitionStateInfo.replicationFactor)
    if (partition.makeLeader(controllerId, topic, partitionId, leaderIsrAndControllerEpoch, correlationId)) {
      // also add this partition to the list of partitions for which the leader is the current broker
      leaderPartitionsLock synchronized {
        leaderPartitions += partition
      }
    }
    trace("Broker %d completed become-leader transition for partition [%s,%d]".format(localBrokerId, topic, partitionId))
  }

  def getOrCreatePartition(topic: String, partitionId: Int, replicationFactor: Int): Partition = {
    var partition = allPartitions.get((topic, partitionId))
    if (partition == null) {
      allPartitions.putIfNotExists((topic, partitionId), new Partition(topic, partitionId, replicationFactor, time, this))
      partition = allPartitions.get((topic, partitionId))
    }
    partition
  }


  private def makeFollower(controllerId: Int, epoch: Int, topic: String, partitionId: Int,
                           partitionStateInfo: PartitionStateInfo, leaders: Set[Broker], correlationId: Int) {
    val leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch
    trace(("Broker %d received LeaderAndIsr request correlationId %d from controller %d epoch %d " +
      "starting the become-follower transition for partition [%s,%d]")
      .format(localBrokerId, correlationId, controllerId, epoch, topic, partitionId))

    val partition = getOrCreatePartition(topic, partitionId, partitionStateInfo.replicationFactor)
    if (partition.makeFollower(controllerId, topic, partitionId, leaderIsrAndControllerEpoch, leaders, correlationId)) {
      // remove this replica's partition from the ISR expiration queue
      leaderPartitionsLock synchronized {
        leaderPartitions -= partition
      }
    }
    trace("Broker %d completed the become-follower transition for partition [%s,%d]".format(localBrokerId, topic, partitionId))
  }

  def becomeLeaderOrFollower(leaderAndISRRequest: LeaderAndIsrRequest): (collection.Map[(String, Int), Short], Short) = {
    val map = leaderAndISRRequest.partitionStateInfoMap
    map.foreach(p =>
      trace("Broker %d handling LeaderAndIsr request correlation id %d received from controller %d epoch %d for partition [%s,%d]"
        .format(localBrokerId, leaderAndISRRequest.correlationId, leaderAndISRRequest.controllerId,
          leaderAndISRRequest.controllerEpoch, p._1._1, p._1._2)))
    info("Handling LeaderAndIsr request %s".format(leaderAndISRRequest))

    val responseMap = new collection.mutable.HashMap[(String, Int), Short]
    if(leaderAndISRRequest.controllerEpoch < controllerEpoch) {
      warn("Broker %d received LeaderAndIsr request correlation id %d with an old controller epoch %d. Latest known controller epoch is %d"
        .format(localBrokerId, leaderAndISRRequest.controllerEpoch, leaderAndISRRequest.correlationId, controllerEpoch))
      (responseMap, ErrorMapping.StaleControllerEpochCode)

    } else {
      val controllerId = leaderAndISRRequest.controllerId
      controllerEpoch = leaderAndISRRequest.controllerEpoch
      for((topicAndPartition, partitionStateInfo) <- leaderAndISRRequest.partitionStateInfoMap) {
        var errorCode = ErrorMapping.NoError
        val topic = topicAndPartition._1
        val partitionId = topicAndPartition._2
        val requestedLeaderId = partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leader
        try {
          if(requestedLeaderId == config.brokerId)
            makeLeader(controllerId, controllerEpoch, topic, partitionId, partitionStateInfo, leaderAndISRRequest.correlationId)
          else
            makeFollower(controllerId, controllerEpoch, topic, partitionId, partitionStateInfo, leaderAndISRRequest.leaders,
              leaderAndISRRequest.correlationId)
        } catch {
          case e: Throwable =>
            val errorMsg = ("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %d " +
              "epoch %d for partition %s").format(localBrokerId, leaderAndISRRequest.correlationId, leaderAndISRRequest.controllerId,
              leaderAndISRRequest.controllerEpoch, topicAndPartition)
            error(errorMsg, e)
            errorCode = ErrorMapping.UnknownCode
        }
        info("Handled leader and isr request %s".format(leaderAndISRRequest))
        // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
        // have been completely populated before starting the checkpointing there by avoiding weird race conditions
//        if (!hwThreadInitialized) {
//          startHighWaterMarksCheckPointThread()
//          hwThreadInitialized = true
//        }
        responseMap.put(topicAndPartition, errorCode)
      }
    }
    (responseMap, ErrorMapping.NoError)
  }

  def startup() = {

  }


}

class ReplicaFetcherManager {
  def addFetcher(topic: String, partitionId: Int, logEndOffset: Long, leaderBroker: ZkUtils.Broker) = {

  }

  def removeFetcher(topic: String, partitionId: Int) = {}

}
