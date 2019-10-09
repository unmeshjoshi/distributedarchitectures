package org.dist.simplekafka

import org.dist.queue.utils.ZkUtils.Broker

case class PartitionLeaderInfo(partitionId:Int, leaderBrokerId:Int, replicaIds:List[Int])

class Controller(val zookeeperClient: ZookeeperClient, val brokerId: Int) {
  def addBroker(broker: Broker) = {
    liveBrokers += broker
  }

  var liveBrokers:Set[Broker] = Set()

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

  def onTopicChange(partitionReplicas:Seq[PartitionReplicas]) = {
    val leaderAndReplicas = partitionReplicas.map(p => {
      PartitionLeaderInfo(p.partitionId, p.brokerIds.head, p.brokerIds)
    })
  }

  //We do not do anything, assuming all topics are created after all the brokers are up and running
  def onBrokerStartup(toSeq: Seq[Int]) = {

  }
}


