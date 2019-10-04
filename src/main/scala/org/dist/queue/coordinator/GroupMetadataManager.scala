package org.dist.queue.coordinator

import org.dist.queue.common.Topic
import org.dist.queue.server.{Config, KafkaZooKeeper, ReplicaManager}

class GroupMetadataManager(replicaManager: ReplicaManager,
                           kafkaZooKeeper: KafkaZooKeeper,
                           config:Config) {
  private def getGroupMetadataTopicPartitionCount: Int = {
    kafkaZooKeeper.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(config.offsetsTopicNumPartitions)
  }
}
