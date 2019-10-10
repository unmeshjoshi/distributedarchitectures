package org.dist.simplekafka

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker

case class PartitionInfo(leader:Broker, allReplicas:List[Broker])

case class LeaderAndReplicas(topicPartition:TopicAndPartition, partitionStateInfo:PartitionInfo)

case class LeaderAndReplicaRequest(leaderReplicas:List[LeaderAndReplicas])
