package org.dist.simplekafka

import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks

class SimpleKafkaApiTest extends ZookeeperTestHarness {
  test("should create leader and follower replicas") {
    val config = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new ReplicaManager(config)
    val simpleKafkaApi = new SimpleKafkaApi(config, replicaManager)
    val leaderAndReplicas = LeaderAndReplicaRequest(List(LeaderAndReplicas(TopicAndPartition("topic1", 0),PartitionInfo(0, List(0, 1)))))
    val request = RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicas), 1)
    simpleKafkaApi.handle(request)
  }
}
