package org.dist.queue.server

import java.util.concurrent.atomic.AtomicBoolean

import org.I0Itec.zkclient.ZkClient
import org.dist.queue.api.LeaderAndIsrRequest
import org.dist.queue.cluster.{Partition, Replica}
import org.dist.queue.common._
import org.dist.queue.controller.{Controller, PartitionStateInfo}
import org.dist.queue.log.LogManager
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.utils.{Time, ZkUtils}

import scala.collection.{Set, mutable}


object ReplicaManager {
  val UnknownLogEndOffset = -1L
}

class ReplicaManager(val config: Config,
                     time: Time,
                     val zkClient: ZkClient,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean ) extends Logging {

  def checkpointHighWatermarks() {
    val replicas = allPartitions.values.map(_.getReplica(config.brokerId)).collect{case Some(replica) => replica}
    val replicasByDir = replicas.filter(_.log.isDefined).groupBy(_.log.get.dir.getParent)
    for((dir, reps) <- replicasByDir) {
      val hwms = reps.map(r => (TopicAndPartition(r.topic, r.partitionId) -> r.highWatermark)).toMap
      highWatermarkCheckpoints(dir).write(hwms)
    }
  }

  def shutdown() {
    info("Shut down")
    replicaFetcherManager.shutdown()
    checkpointHighWatermarks()
    info("Shutted down completely")
  }


  val highWatermarkCheckpoints = config.logDirs.map(dir => (dir, new HighwaterMarkCheckpoint(dir))).toMap



  def getReplica(topic: String, partitionId: Int, replicaId: Int = config.brokerId): Option[Replica] =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None => None
      case Some(partition) => partition.getReplica(replicaId)
    }
  }

  def getReplicaOrException(topic: String, partition: Int): Replica = {
    val replicaOpt = getReplica(topic, partition)
    if(replicaOpt.isDefined)
      return replicaOpt.get
    else
      throw new ReplicaNotAvailableException("Replica %d is not available for partition [%s,%d]".format(config.brokerId, topic, partition))
  }

  def getLeaderReplicaIfLocal(topic: String, partitionId: Int): Replica =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None =>
        throw new UnknownTopicOrPartitionException("Partition [%s,%d] doesn't exist on %d".format(topic, partitionId, config.brokerId))
      case Some(partition) =>
        partition.leaderReplicaIfLocal match {
          case Some(leaderReplica) => leaderReplica
          case None =>
            throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
              .format(topic, partitionId, config.brokerId))
        }
    }
  }


  /**
   * This function is only used in two places: in Partition.updateISR() and KafkaApis.handleProducerRequest().
   * In the former case, the partition should have been created, in the latter case, return -1 will put the request into purgatory
   */
  def getReplicationFactorForPartition(topic: String, partitionId: Int) = {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case Some(partition) =>
        partition.replicationFactor
      case None =>
        -1
    }
  }


  def getPartition(topic: String, partitionId: Int): Option[Partition] = {
    val partition = allPartitions.get((topic, partitionId))
    if (partition == null)
      None
    else
      Some(partition)
  }

  val replicaFetcherManager = new ReplicaFetcherManager(this, config)

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
                           partitionStateInfo: PartitionStateInfo, leaders: Set[Broker], correlationId: Int) = {
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
    debug("Handling LeaderAndIsr request %s".format(leaderAndISRRequest))

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


case class BrokerAndFetcherId(broker: Broker, fetcherId: Int)

class ReplicaFetcherManager(replicaManager:ReplicaManager, config:Config, numFetchers: Int = 1) extends Logging {
  def shutdown() = {}

  // map of (source brokerid, fetcher Id per source broker) => fetcher
  private val fetcherThreadMap = new mutable.HashMap[BrokerAndFetcherId, FetcherThread]
  private val mapLock = new Object

  def createFetcherThread(fetcherId: Int, sourceBroker: ZkUtils.Broker): FetcherThread = {
    new FetcherThread(s"Fetcher-${fetcherId}", replicaManager, sourceBroker, config)
  }


  private def getFetcherId(topic: String, partitionId: Int) : Int = {
    (topic.hashCode() + 31 * partitionId) % numFetchers
  }

  def addFetcher(topic: String, partitionId: Int, initialOffset: Long, leaderBroker: ZkUtils.Broker) = {
    mapLock synchronized {
      var fetcherThread: FetcherThread = null
      val key = BrokerAndFetcherId(leaderBroker, getFetcherId(topic, partitionId))
      fetcherThreadMap.get(key) match {
        case Some(f) => fetcherThread = f
        case None =>
          fetcherThread = createFetcherThread(key.fetcherId, leaderBroker)
          fetcherThreadMap.put(key, fetcherThread)
          fetcherThread.start
      }
      fetcherThread.addPartition(topic, partitionId, initialOffset)
      info("Adding fetcher for partition [%s,%d], initOffset %d to broker %d with fetcherId %d"
        .format(topic, partitionId, initialOffset, leaderBroker.id, key.fetcherId))
    }
  }

  def removeFetcher(topic: String, partitionId: Int) = {

  }

}
