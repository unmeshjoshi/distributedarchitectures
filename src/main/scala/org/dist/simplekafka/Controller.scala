package org.dist.simplekafka

case class PartitionLeaderInfo(partitionId:Int, leaderBrokerId:Int, replicaIds:List[Int])

class Controller(zookeeperClient: ZookeeperClient, brokerId: Int) {
  def elect() = {
    val leaderId = s"${brokerId}"
    try {
      zookeeperClient.tryCreatingControllerPath(leaderId)
      onBecomingLeader()
    } catch {
      case e: ControllerExistsException ⇒ e.controllerId
    }
  }

  def onBecomingLeader() = {
    zookeeperClient.subscribeTopicChangeListener(new TopicChangeHandler(zookeeperClient, onTopicChange))
  }

  def onTopicChange(partitionReplicas:Seq[PartitionReplicas]) = {
    val leaderAndReplicas = partitionReplicas.map(p ⇒ {
      PartitionLeaderInfo(p.partitionId, p.brokerIds.head, p.brokerIds)
    })
  }
}


