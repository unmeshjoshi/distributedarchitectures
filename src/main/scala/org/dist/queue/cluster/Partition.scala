package org.dist.queue.cluster

import org.dist.queue.common.{Logging, NotLeaderForPartitionException, Pool}
import org.dist.queue.controller.{Controller, LeaderAndIsr, LeaderIsrAndControllerEpoch}
import org.dist.queue.message.ByteBufferMessageSet
import org.dist.queue.server.ReplicaManager
import org.dist.queue.utils.Time
import org.dist.queue.utils.ZkUtils.Broker

import scala.collection.Set

class Partition(val topic: String,
                val partitionId: Int,
                var replicationFactor: Int,
                time: Time,
                val replicaManager: ReplicaManager) extends Logging {
  def appendMessagesToLeader(messages: ByteBufferMessageSet): (Long, Long) = {
    leaderIsrUpdateLock synchronized {
      val leaderReplicaOpt = leaderReplicaIfLocal()
      leaderReplicaOpt match {
        case Some(leaderReplica) =>
          val log = leaderReplica.log.get
          val (start, end) = log.append(messages, assignOffsets = true)
          // we may need to increment high watermark since ISR could be down to 1
          maybeIncrementLeaderHW(leaderReplica)
          (start, end)
        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
            .format(topic, partitionId, localBrokerId))
      }
    }
  }

  def leaderReplicaIfLocal(): Option[Replica] = {
    leaderIsrUpdateLock synchronized {
      leaderReplicaIdOpt match {
        case Some(leaderReplicaId) =>
          if (leaderReplicaId == localBrokerId)
            getReplica(localBrokerId)
          else
            None
        case None => None
      }
    }
  }

  private def maybeIncrementLeaderHW(leaderReplica: Replica) {
    val allLogEndOffsets: Set[Long] = inSyncReplicas.map(_.logEndOffset)
    val newHighWatermark = allLogEndOffsets.min
    val oldHighWatermark = leaderReplica.highWatermark
    if(newHighWatermark > oldHighWatermark) {
      leaderReplica.highWatermark = newHighWatermark
      debug("Highwatermark for partition [%s,%d] updated to %d".format(topic, partitionId, newHighWatermark))
    }
    else
      debug("Old hw for partition [%s,%d] is %d. New hw is %d. All leo's are %s"
        .format(topic, partitionId, oldHighWatermark, newHighWatermark, allLogEndOffsets.mkString(",")))
  }


  /**
   *  If the leaderEpoch of the incoming request is higher than locally cached epoch, make the local replica the follower in the following steps.
   *  1. stop any existing fetcher on this partition from the local replica
   *  2. make sure local replica exists and truncate the log to high watermark
   *  3. set the leader and set ISR to empty
   *  4. start a fetcher to the new leader
   */
  def makeFollower(controllerId: Int, topic: String, partitionId: Int, leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                   leaders: Set[Broker], correlationId: Int): Boolean = {
    leaderIsrUpdateLock synchronized {
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
      if (leaderEpoch >= leaderAndIsr.leaderEpoch) {
        trace(("Broker %d discarded the become-follower request with correlation id %d from " +
          "controller %d epoch %d for partition [%s,%d] since current leader epoch %d is >= the request's leader epoch %d")
          .format(localBrokerId, correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch, topic,
            partitionId, leaderEpoch, leaderAndIsr.leaderEpoch))
        return false
      }
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
      // make sure local replica exists. This reads the last check pointed high watermark from disk. On startup, it is
      // important to ensure that this operation happens for every single partition in a leader and isr request, else
      // some high watermark values could be overwritten with 0. This leads to replicas fetching from the earliest offset
      // on the leader
      val localReplica = getOrCreateReplica()
      val newLeaderBrokerId: Int = leaderAndIsr.leader
      // TODO: Delete leaders from LeaderAndIsrRequest in 0.8.1
      leaders.find(_.id == newLeaderBrokerId) match {
        case Some(leaderBroker) =>
          // stop fetcher thread to previous leader
          replicaFetcherManager.removeFetcher(topic, partitionId)
          localReplica.log.get.truncateTo(localReplica.highWatermark)
          inSyncReplicas = Set.empty[Replica]
          leaderEpoch = leaderAndIsr.leaderEpoch
          zkVersion = leaderAndIsr.zkVersion
          leaderReplicaIdOpt = Some(newLeaderBrokerId)
          if (!replicaManager.isShuttingDown.get()) {
            // start fetcher thread to current leader if we are not shutting down
            replicaFetcherManager.addFetcher(topic, partitionId, localReplica.logEndOffset, leaderBroker)
          }
          else {
            trace(("Broker %d ignored the become-follower state change with correlation id %d from " +
              "controller %d epoch %d since it is shutting down")
              .format(localBrokerId, correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch))
          }
        case None => // we should not come here
          error(("Broker %d aborted the become-follower state change with correlation id %d from " +
            "controller %d epoch %d for partition [%s,%d] new leader %d")
            .format(localBrokerId, correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch,
              topic, partitionId, newLeaderBrokerId))
      }
      true
    }
  }


  private val localBrokerId = replicaManager.config.brokerId
  private val logManager = replicaManager.logManager
  private val replicaFetcherManager = replicaManager.replicaFetcherManager
  private val zkClient = replicaManager.zkClient
  var leaderReplicaIdOpt: Option[Int] = None
  var inSyncReplicas: Set[Replica] = Set.empty[Replica]
  private val assignedReplicaMap = new Pool[Int,Replica]
  private val leaderIsrUpdateLock = new Object
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = Controller.InitialControllerEpoch - 1
  this.logIdent = "Partition [%s,%d] on broker %d: ".format(topic, partitionId, localBrokerId)

  private def isReplicaLocal(replicaId: Int) : Boolean = (replicaId == localBrokerId)

  def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
    val replicaOpt = getReplica(replicaId)
    replicaOpt match {
      case Some(replica) => replica
      case None =>
        if (isReplicaLocal(replicaId)) {
          val log = logManager.getOrCreateLog(topic, partitionId)
          val offset = replicaManager.highWatermarkCheckpoints(log.dir.getParent).read(topic, partitionId).min(log.logEndOffset)
          val localReplica = new Replica(replicaId, this, time, offset, Some(log))
          addReplicaIfNotExists(localReplica)
        }
        else {
          val remoteReplica = new Replica(replicaId, this, time)
          addReplicaIfNotExists(remoteReplica)
        }
        getReplica(replicaId).get
    }
  }


  def addReplicaIfNotExists(replica: Replica) = {
    assignedReplicaMap.putIfNotExists(replica.brokerId, replica)
  }

  def assignedReplicas(): Set[Replica] = {
    assignedReplicaMap.values.toSet
  }


  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = {
    val replica = assignedReplicaMap.get(replicaId)
    if (replica == null)
      None
    else
      Some(replica)
  }
  /**
   *  If the leaderEpoch of the incoming request is higher than locally cached epoch, make the local replica the leader in the following steps.
   *  1. stop the existing replica fetcher
   *  2. create replicas in ISR if needed (the ISR expand/shrink logic needs replicas in ISR to be available)
   *  3. reset LogEndOffset for remote replicas (there could be old LogEndOffset from the time when this broker was the leader last time)
   *  4. set the new leader and ISR
   */
  def makeLeader(controllerId: Int, topic: String, partitionId: Int,
                 leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch, correlationId: Int): Boolean = {
    leaderIsrUpdateLock synchronized {
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
      if (leaderEpoch >= leaderAndIsr.leaderEpoch){
        trace(("Broker %d discarded the become-leader request with correlation id %d from " +
          "controller %d epoch %d for partition [%s,%d] since current leader epoch %d is >= the request's leader epoch %d")
          .format(localBrokerId, correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch, topic,
            partitionId, leaderEpoch, leaderAndIsr.leaderEpoch))
        return false
      }
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
      // stop replica fetcher thread, if any
      replicaFetcherManager.removeFetcher(topic, partitionId)

      val newInSyncReplicas = leaderAndIsr.isr.map(r => getOrCreateReplica(r)).toSet
      // reset LogEndOffset for remote replicas
      assignedReplicas.foreach(r => if (r.brokerId != localBrokerId) r.logEndOffset = ReplicaManager.UnknownLogEndOffset)
      inSyncReplicas = newInSyncReplicas
      leaderEpoch = leaderAndIsr.leaderEpoch
      zkVersion = leaderAndIsr.zkVersion
      leaderReplicaIdOpt = Some(localBrokerId)
      // we may need to increment high watermark since ISR could be down to 1
//      maybeIncrementLeaderHW(getReplica().get)
      true
    }
  }

}

