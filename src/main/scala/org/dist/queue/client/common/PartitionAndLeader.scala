package org.dist.queue.client.common

case class PartitionAndLeader(topic: String, partitionId: Int, leaderBrokerIdOpt: Option[Int])
