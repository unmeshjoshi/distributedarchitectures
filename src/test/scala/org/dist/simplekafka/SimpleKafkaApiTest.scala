package org.dist.simplekafka

import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks

class SimpleKafkaApiTest extends ZookeeperTestHarness {

  test("should create leader and follower replicas") {
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new ReplicaManager(config)
    val simpleKafkaApi = new SimpleKafkaApi(config, replicaManager)
    val leaderAndReplicas = LeaderAndReplicaRequest(List(LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(0, List(0, 1)))))
    val request = RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicas), 1)
    simpleKafkaApi.handle(request)
    assert(replicaManager.allPartitions.size() == 1)
    val partition = replicaManager.allPartitions.get(TopicAndPartition("topic1", 0))
    assert(partition.logFile.exists())
  }

  test("Update metadata cache") {
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new ReplicaManager(config)
    val simpleKafkaApi = new SimpleKafkaApi(config, replicaManager)
    val aliveBrokers = List(Broker(0, "10.10.10.10", 8000), Broker(0, "10.10.10.11", 8000), Broker(0, "10.10.10.12", 8000))
    val updateMetadataRequest = new UpdateMetadataRequest(aliveBrokers, List(LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(0, List(0, 1))),
      LeaderAndReplicas(TopicAndPartition("topic1", 1), PartitionInfo(1, List(0, 1)))))

    val request = RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest), 1)

    simpleKafkaApi.handle(request)

    assert(simpleKafkaApi.aliveBrokers == aliveBrokers)
    assert(simpleKafkaApi.leaderCache.get(TopicAndPartition("topic1", 0)) == PartitionInfo(0, List(0, 1)))
  }

  test("should get topic metadata for given topic") {
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new ReplicaManager(config)
    val simpleKafkaApi = new SimpleKafkaApi(config, replicaManager)
    val aliveBrokers = List(Broker(0, "10.10.10.10", 8000), Broker(0, "10.10.10.11", 8000), Broker(0, "10.10.10.12", 8000))
    val request = RequestOrResponse(RequestKeys.MetadataKey, JsonSerDes.serialize(TopicMetadataRequest("topic1")), 1)
    simpleKafkaApi.aliveBrokers = aliveBrokers
    simpleKafkaApi.leaderCache = new java.util.HashMap[TopicAndPartition, PartitionInfo]
    simpleKafkaApi.leaderCache.put(TopicAndPartition("topic1", 0), PartitionInfo(0, List(0, 1)))
    simpleKafkaApi.leaderCache.put(TopicAndPartition("topic1", 1), PartitionInfo(1, List(0, 1)))

    val response = simpleKafkaApi.handle(request)
    val topicMetadataResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes, classOf[TopicMetadataResponse])

    assert(topicMetadataResponse.topicPartitions.size == 2)
    assert(topicMetadataResponse.topicPartitions(TopicAndPartition("topic1", 0)) == PartitionInfo(0, List(0, 1)))
    assert(topicMetadataResponse.topicPartitions(TopicAndPartition("topic1", 1)) == PartitionInfo(1, List(0, 1)))
  }
}
