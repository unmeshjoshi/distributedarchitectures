package org.dist.simplekafka

import java.util.concurrent.atomic.AtomicInteger

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker

class Controller(val zookeeperClient: ZookeeperClient, val brokerId: Int, socketServer: SimpleSocketServer) {
  val correlationId = new AtomicInteger(0)
  var liveBrokers: Set[Broker] = Set()

  def elect() = {
    val leaderId = s"${brokerId}"
    try {
      zookeeperClient.tryCreatingControllerPath(leaderId)
      onBecomingLeader()
    } catch {
      case e: ControllerExistsException => e.controllerId
    }
  }

  def onBecomingLeader() = {
    liveBrokers = liveBrokers ++ zookeeperClient.getAllBrokers()
    zookeeperClient.subscribeTopicChangeListener(new TopicChangeHandler(zookeeperClient, onTopicChange))
    zookeeperClient.subscribeBrokerChangeListener(new BrokerChangeListener(this, zookeeperClient))
  }

  def onTopicChange(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
    val leaderAndReplicas: Seq[LeaderAndReplicas] = partitionReplicas.map(p => {
      val leaderBrokerId = p.brokerIds.head //This is where leader for particular partition is selected
      LeaderAndReplicas(TopicAndPartition(topicName, p.partitionId), PartitionInfo(leaderBrokerId, p.brokerIds))
    })

    sendLeaderAndReplicaRequest(leaderAndReplicas, partitionReplicas)
    sendUpdateMetadataRequest(leaderAndReplicas)

  }

  private def sendUpdateMetadataRequest(leaderAndReplicas: Seq[LeaderAndReplicas]) = {
    liveBrokers.foreach(broker ⇒ {
      val updateMetadataRequest = UpdateMetadataRequest(liveBrokers.toList, leaderAndReplicas.toList)
      val request = RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest), correlationId.incrementAndGet())
      socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
    })
  }

  private def sendLeaderAndReplicaRequest(leaderAndReplicas:Seq[LeaderAndReplicas], partitionReplicas: Seq[PartitionReplicas]) = {
    val brokersForPartition: Set[Broker] = partitionReplicas.flatMap(p ⇒ p.brokerIds).toSet.map((bid: Int) ⇒ liveBrokers.find(b ⇒ b.id == bid).get)

    brokersForPartition.foreach(broker ⇒ {
      val leaderAndReplicaRequest = LeaderAndReplicaRequest(leaderAndReplicas.toList)
      val request = RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicaRequest), correlationId.getAndIncrement())
      socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
    })
  }

  def addBroker(broker: Broker) = {
    liveBrokers += broker
  }

  //We do not do anything, assuming all topics are created after all the brokers are up and running
  def onBrokerStartup(toSeq: Seq[Int]) = {

  }
}


