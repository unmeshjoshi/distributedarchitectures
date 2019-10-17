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
    val leaderAndReplicas = LeaderAndReplicaRequest(List(LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))))
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
    val updateMetadataRequest = new UpdateMetadataRequest(aliveBrokers,
      List(
      LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000)))),
      LeaderAndReplicas(TopicAndPartition("topic1", 1), PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))))

    val request = RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest), 1)

    simpleKafkaApi.handle(request)

    assert(simpleKafkaApi.aliveBrokers == aliveBrokers)
    assert(simpleKafkaApi.leaderCache.get(TopicAndPartition("topic1", 0)) == PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))
  }

  test("should get topic metadata for given topic") {
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new ReplicaManager(config)
    val simpleKafkaApi = new SimpleKafkaApi(config, replicaManager)
    val aliveBrokers = List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000), Broker(2, "10.10.10.12", 8000))
    val request = RequestOrResponse(RequestKeys.GetMetadataKey, JsonSerDes.serialize(TopicMetadataRequest("topic1")), 1)
    simpleKafkaApi.aliveBrokers = aliveBrokers
    simpleKafkaApi.leaderCache = new java.util.HashMap[TopicAndPartition, PartitionInfo]
    simpleKafkaApi.leaderCache.put(TopicAndPartition("topic1", 0), PartitionInfo(Broker(0, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))
    simpleKafkaApi.leaderCache.put(TopicAndPartition("topic1", 1), PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))

    val response = simpleKafkaApi.handle(request)
    val topicMetadataResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes, classOf[TopicMetadataResponse])

    assert(topicMetadataResponse.topicPartitions.size == 2)
    assert(topicMetadataResponse.topicPartitions(TopicAndPartition("topic1", 0)) == PartitionInfo(Broker(0, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))
    assert(topicMetadataResponse.topicPartitions(TopicAndPartition("topic1", 1)) == PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))
  }

  test("should handle produce request") {
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new ReplicaManager(config)
    val simpleKafkaApi = new SimpleKafkaApi(config, replicaManager)

    val leaderAndReplicas = LeaderAndReplicaRequest(List(LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))))
    simpleKafkaApi.handle(RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicas), 1))
    assert(replicaManager.allPartitions.size() == 1)

    val request = RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(ProduceRequest(TopicAndPartition("topic1", 0), "key1", "message")), 1)
    val response = simpleKafkaApi.handle(request)
    val produceResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[ProduceResponse])
    assert(produceResponse.offset == 1)
  }

  test("multiple produce requests should increment offset") {
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new ReplicaManager(config)
    val simpleKafkaApi = new SimpleKafkaApi(config, replicaManager)

    val leaderAndReplicas = LeaderAndReplicaRequest(List(LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))))
    simpleKafkaApi.handle(RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicas), 1))
    assert(replicaManager.allPartitions.size() == 1)

    val request1 = RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(ProduceRequest(TopicAndPartition("topic1", 0), "key1", "message1")), 1)
    val response1 = simpleKafkaApi.handle(request1)
    val produceResponse1 = JsonSerDes.deserialize(response1.messageBodyJson.getBytes(), classOf[ProduceResponse])
    assert(produceResponse1.offset == 1)

    val request2 = RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(ProduceRequest(TopicAndPartition("topic1", 0), "key2", "message2")), 1)
    val response2 = simpleKafkaApi.handle(request2)
    val produceResponse2 = JsonSerDes.deserialize(response2.messageBodyJson.getBytes(), classOf[ProduceResponse])
    assert(produceResponse2.offset == 2)
  }

  test("should get no messages when nothing is produced on the topic partition") {
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new ReplicaManager(config)
    val simpleKafkaApi = new SimpleKafkaApi(config, replicaManager)

    val leaderAndReplicas = LeaderAndReplicaRequest(List(LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))))
    simpleKafkaApi.handle(RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicas), 1))
    assert(replicaManager.allPartitions.size() == 1)

    val consumeRequest = ConsumeRequest(TopicAndPartition("topic1", 0))
    val request = RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(consumeRequest), 1)
    val consumeResponse = simpleKafkaApi.handle(request)
    val response = JsonSerDes.deserialize(consumeResponse.messageBodyJson.getBytes(), classOf[ConsumeResponse])
    assert(response.messages.size == 0)
  }

  test("should consume messages from start offset") {
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new ReplicaManager(config)
    val simpleKafkaApi = new SimpleKafkaApi(config, replicaManager)

    val leaderAndReplicas = LeaderAndReplicaRequest(List(LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))))
    simpleKafkaApi.handle(RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicas), 1))
    assert(replicaManager.allPartitions.size() == 1)


    produceTwoMessages(simpleKafkaApi)

    val consumeRequest = ConsumeRequest(TopicAndPartition("topic1", 0))
    val request = RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(consumeRequest), 1)
    val consumeResponse = simpleKafkaApi.handle(request)
    val response = JsonSerDes.deserialize(consumeResponse.messageBodyJson.getBytes(), classOf[ConsumeResponse])
    assert(response.messages.size == 2)
    assert(response.messages.get("key1") == Some("message1"))
    assert(response.messages.get("key2") == Some("message2"))
  }

  private def produceTwoMessages(simpleKafkaApi: SimpleKafkaApi) = {
    val request1 = RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(ProduceRequest(TopicAndPartition("topic1", 0), "key1", "message1")), 1)
    val response1 = simpleKafkaApi.handle(request1)
    val produceResponse1 = JsonSerDes.deserialize(response1.messageBodyJson.getBytes(), classOf[ProduceResponse])
    assert(produceResponse1.offset == 1)

    val request2 = RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(ProduceRequest(TopicAndPartition("topic1", 0), "key2", "message2")), 1)
    val response2 = simpleKafkaApi.handle(request2)
    val produceResponse2 = JsonSerDes.deserialize(response2.messageBodyJson.getBytes(), classOf[ProduceResponse])
    assert(produceResponse2.offset == 2)
  }
}
