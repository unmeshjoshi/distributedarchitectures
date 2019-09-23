package org.dist.queue

import org.I0Itec.zkclient.ZkClient
import org.dist.queue.utils.ZkUtils

class CreateTopicCommand {
  def createTopic(zkClient:ZkClient, topicName:String, numPartitions:Int = 1, replicationFactor: Int = 1, replicaAssignmentStr: String = ""): Unit = {
    val brokerList = ZkUtils.getSortedBrokerList(zkClient)
    val partitionReplicaAssignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor)
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(topicName, partitionReplicaAssignment, zkClient)
  }
}
