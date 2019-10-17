package org.dist.simplekafka

import java.util.concurrent.atomic.AtomicInteger

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.{Logging, TopicAndPartition}
import org.dist.queue.utils.Utils

class SimpleProducer(bootstrapBroker: InetAddressAndPort, socketClient:SocketClient = new SocketClient) extends Logging {
  val correlationId = new AtomicInteger(0)

  def produce(topic: String, key: String, message: String) = {
    val topicPartitions = fetchTopicMetadata(topic)
    val partitionId = partitionFor(key, topicPartitions.size)
    val leaderBroker = leaderFor(topic, partitionId, topicPartitions)

    val produceRequest = ProduceRequest(TopicAndPartition(topic, partitionId), key, message)
    val producerRequest = RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(produceRequest), correlationId.incrementAndGet())
    val produceResponse = socketClient.sendReceiveTcp(producerRequest, InetAddressAndPort.create(leaderBroker.host, leaderBroker.port))
    val response1 = JsonSerDes.deserialize(produceResponse.messageBodyJson.getBytes(), classOf[ProduceResponse])
    info(s"Produced message ${key} -> ${message} on leader broker ${leaderBroker}. Message offset is ${response1.offset}")
    response1.offset
  }

  private def fetchTopicMetadata(topic: String) = {
    val request = RequestOrResponse(RequestKeys.GetMetadataKey, JsonSerDes.serialize(TopicMetadataRequest(topic)), 1)

    val response = socketClient.sendReceiveTcp(request, bootstrapBroker)
    val topicMetadataResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[TopicMetadataResponse])
    val topicPartitions: Map[TopicAndPartition, PartitionInfo] = topicMetadataResponse.topicPartitions
    topicPartitions
  }

  def partitionFor(key: String, numPartitions: Int) = {
    Utils.abs(key.hashCode) % numPartitions
  }

  def leaderFor(topicName: String, partitionId: Int, topicPartitions: Map[TopicAndPartition, PartitionInfo]) = {
    val info: PartitionInfo = topicPartitions(TopicAndPartition(topicName, partitionId))
    info.leader
  }
}
