package org.dist.simplekafka

import java.util

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config

class ReplicaManager(config:Config) {
  val allPartitions = new util.HashMap[TopicAndPartition, Partition]()

  def makeFollower(topicAndPartition: TopicAndPartition, leaderId:Int) = {
    val partition = getOrCreatePartition(topicAndPartition)
    partition.makeFollower(leaderId)
  }

  def makeLeader(topicAndPartition: TopicAndPartition) = {
    val partition = getOrCreatePartition(topicAndPartition)
    partition.makeLeader()
  }

  def getPartition(topicAndPartition: TopicAndPartition) = {
    allPartitions.get(topicAndPartition)
  }

  def getOrCreatePartition(topicAndPartition: TopicAndPartition) = {
    var partition = allPartitions.get(topicAndPartition)
    if (null == partition) {
      partition = new Partition(config, topicAndPartition)
      allPartitions.put(topicAndPartition, partition)
    }
    partition
  }

}
