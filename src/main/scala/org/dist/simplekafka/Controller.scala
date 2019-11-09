package org.dist.simplekafka

import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.annotations.VisibleForTesting
import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker

import scala.util.control.Breaks

class Controller(val zookeeperClient: ZookeeperClient, val brokerId: Int, socketServer: SimpleSocketServer) {

  val correlationId = new AtomicInteger(0)
  var liveBrokers: Set[Broker] = Set()
  var currentLeader = -1

  def startup(): Unit = {
    zookeeperClient.subscribeControllerChangeListner(this)
    elect()
  }

  def electNewLeaderForPartition() = {
    val topics: List[String] = zookeeperClient.getTopics()
    var newLeaderAndReplicaList: List[LeaderAndReplicas] = List();
    var partitionReplicas: List[PartitionReplicas] = List();

    topics.foreach(topicName => {
      var leaderAndReplicaList: List[LeaderAndReplicas] = zookeeperClient.getPartitionReplicaLeaderInfo(topicName);

      var i : Int = 0;
      var leaderChanged: Boolean = false;
      leaderAndReplicaList.foreach(leaderAndReplica => {
        val partitionInfo = leaderAndReplica.partitionStateInfo
        var partitionCurrentLeader: Broker = partitionInfo.leader;

        partitionReplicas = partitionReplicas ++ zookeeperClient.getPartitionAssignmentsFor(topicName);

        //If the leader is not alive, choose new leader from replica set
        //Replace the new leaderAndReplica object in the list
        if (!liveBrokers.contains(partitionCurrentLeader)) {
          val loop = new Breaks;
          loop.breakable {
            partitionInfo.allReplicas.foreach(replica => {
              if (!partitionCurrentLeader.equals(replica)) {
                partitionCurrentLeader = replica;

                val newPartitionInfo: PartitionInfo = PartitionInfo(partitionCurrentLeader, partitionInfo.allReplicas);
                val leaderReplica: LeaderAndReplicas = LeaderAndReplicas(leaderAndReplica.topicPartition, newPartitionInfo);

                leaderAndReplicaList = leaderAndReplicaList.patch(i,Seq(leaderReplica),1);
                leaderChanged = true;

                loop.break();
              }
            })
          }
        }
        i = i + 1;
      })
      if(leaderChanged) {
        zookeeperClient.setPartitionLeaderForTopic(topicName, leaderAndReplicaList);
        newLeaderAndReplicaList = newLeaderAndReplicaList ++ leaderAndReplicaList;
      }
    })

    if(newLeaderAndReplicaList.nonEmpty) {
      sendUpdateMetadataRequestToAllLiveBrokers(newLeaderAndReplicaList)
      sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(newLeaderAndReplicaList, partitionReplicas)
    }
  }

  def shutdown() = {
  }

  def elect() = {
    val leaderId = s"${brokerId}"
    try {
      zookeeperClient.tryCreatingControllerPath(leaderId)
      this.currentLeader = brokerId;
      onBecomingLeader()
    } catch {
      case e: ControllerExistsException => {
        this.currentLeader = e.controllerId.toInt
      }
    }
  }

  def onBecomingLeader() = {
    liveBrokers = liveBrokers ++ zookeeperClient.getAllBrokers()
    zookeeperClient.subscribeTopicChangeListener(new TopicChangeHandler(zookeeperClient, onTopicChange))
    zookeeperClient.subscribeBrokerChangeListener(new BrokerChangeListener(this, zookeeperClient))
  }

  def onTopicChange(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
    val leaderAndReplicas: Seq[LeaderAndReplicas] = selectLeaderAndFollowerBrokersForPartitions(topicName, partitionReplicas)

    zookeeperClient.setPartitionLeaderForTopic(topicName, leaderAndReplicas.toList);
    //This is persisted in zookeeper for failover.. we are just keeping it in memory for now.
    sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas, partitionReplicas)
    sendUpdateMetadataRequestToAllLiveBrokers(leaderAndReplicas)

  }

  private def selectLeaderAndFollowerBrokersForPartitions(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
    val leaderAndReplicas: Seq[LeaderAndReplicas] = partitionReplicas.map(p => {
      val leaderBrokerId = p.brokerIds.head //This is where leader for particular partition is selected
      val leaderBroker = getBroker(leaderBrokerId)
      val replicaBrokers = p.brokerIds.map(id ⇒ getBroker(id))
      LeaderAndReplicas(TopicAndPartition(topicName, p.partitionId), PartitionInfo(leaderBroker, replicaBrokers))
    })
    leaderAndReplicas
  }

  private def getBroker(brokerId: Int) = {
    liveBrokers.find(b ⇒ b.id == brokerId).get
  }

  private def sendUpdateMetadataRequestToAllLiveBrokers(leaderAndReplicas: Seq[LeaderAndReplicas]) = {
    val brokerListToIsrRequestMap =
      liveBrokers.foreach(broker ⇒ {
        val updateMetadataRequest = UpdateMetadataRequest(liveBrokers.toList, leaderAndReplicas.toList)
        val request = RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest), correlationId.incrementAndGet())
        socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
      })
  }

  import scala.jdk.CollectionConverters._

  @VisibleForTesting
  def sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas: Seq[LeaderAndReplicas], partitionReplicas: Seq[PartitionReplicas]) = {
    val brokerToLeaderIsrRequest = new util.HashMap[Broker, java.util.List[LeaderAndReplicas]]()
    leaderAndReplicas.foreach(lr ⇒ {
      lr.partitionStateInfo.allReplicas.foreach(broker ⇒ {
        var leaderReplicas = brokerToLeaderIsrRequest.get(broker)
        if (leaderReplicas == null) {
          leaderReplicas = new util.ArrayList[LeaderAndReplicas]()
          brokerToLeaderIsrRequest.put(broker, leaderReplicas)
        }
        leaderReplicas.add(lr)
      })
    })

    val brokers = brokerToLeaderIsrRequest.keySet().asScala
    for (broker ← brokers) {
      val leaderAndReplicas: java.util.List[LeaderAndReplicas] = brokerToLeaderIsrRequest.get(broker)
      val leaderAndReplicaRequest = LeaderAndReplicaRequest(leaderAndReplicas.asScala.toList)
      val request = RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicaRequest), correlationId.getAndIncrement())
      socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
    }
  }

  def addBroker(broker: Broker) = {
    liveBrokers += broker
  }

  //We do not do anything, assuming all topics are created after all the brokers are up and running
  def onBrokerStartup(toSeq: Seq[Int]) = {

  }

  def setCurrent(existingControllerId: Int): Unit = {
    this.currentLeader = existingControllerId
  }
}


