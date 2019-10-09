package org.dist.simplekafka

import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import java.util.HashMap

class SimpleKafkaApi(config:Config, replicaManager:ReplicaManager) {

  val allPartitions = new HashMap[TopicAndPartition, Partition]
  def handle(request: RequestOrResponse): RequestOrResponse = {
    request.requestId match {
      case RequestKeys.LeaderAndIsrKey => {
        val leaderAndReplicasRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[LeaderAndReplicaRequest])
        leaderAndReplicasRequest.leaderReplicas.foreach(leaderAndReplicas ⇒ {
          val topicAndPartition = leaderAndReplicas.topicPartition
          val leader = leaderAndReplicas.partitionStateInfo.leader
          if (leader == config.brokerId)
            replicaManager.makeLeader(topicAndPartition)
          else
            replicaManager.makeFollower(topicAndPartition, leader)
        })
      }
    }

    RequestOrResponse(RequestKeys.LeaderAndIsrKey, "", request.correlationId)
  }
}
