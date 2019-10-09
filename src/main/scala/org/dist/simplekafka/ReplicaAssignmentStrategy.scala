package org.dist.simplekafka

class ReplicaAssignmentStrategy {
  def assignReplica(partitionId: Int, replicationFactor: Int, brokerIds: List[Int]): List[Int] = {
    brokerIds.slice(0, replicationFactor)
  }
}
