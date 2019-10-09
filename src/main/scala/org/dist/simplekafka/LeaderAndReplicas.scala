package org.dist.simplekafka

import org.dist.queue.common.TopicAndPartition

case class PartitionInfo(leader:Int, allReplicas:List[Int])

case class LeaderAndReplicas(topicPartition:TopicAndPartition, partitionStateInfo:PartitionInfo)

case class LeaderAndReplicaRequest(leaderReplicas:List[LeaderAndReplicas])
