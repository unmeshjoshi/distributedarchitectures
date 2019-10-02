package org.dist.queue.controller

import org.dist.queue.common.{Logging, TopicAndPartition}


class OfflinePartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {
  /**
   * @param topicAndPartition   The topic and partition whose leader needs to be elected
   * @param currentLeaderAndIsr The current leader and isr of input partition read from zookeeper
   * @throws NoReplicaOnlineException If no replica in the assigned replicas list is alive
   * @return The leader and isr request, with the newly selected leader info, to send to the brokers
   *         Also, returns the list of replicas the returned leader and isr request should be sent to
   *         This API selects a new leader for the input partition
   */
  override def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    (LeaderAndIsr(1, 1, List(), 1), Seq[Int](1))
  }
}