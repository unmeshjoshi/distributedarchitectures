package org.dist.simplekafka

import java.util.concurrent.atomic.AtomicInteger

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.{Logging, TopicAndPartition}
import org.dist.queue.utils.Utils

class SimpleConsumer(bootstrapBroker: InetAddressAndPort, socketClient:SocketClient = new SocketClient) extends Logging {
  val correlationId = new AtomicInteger(0)

  def consume(topic: String) = {
    val result = new java.util.HashMap[String, String]()
    val topicMetadata: Map[TopicAndPartition, PartitionInfo] = fetchTopicMetadata(topic)
    topicMetadata.foreach(tp ⇒ {
      val topicPartition = tp._1
      val partitionInfo = tp._2
      val leader = partitionInfo.leader
      val request = RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(ConsumeRequest(topicPartition)), correlationId.getAndIncrement())
      val response = socketClient.sendReceiveTcp(request, InetAddressAndPort.create(leader.host, leader.port))
      val consumeResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[ConsumeResponse])
      consumeResponse.messages.foreach(m ⇒ {
        result.put(m._1, m._2)
      })
    })
    result
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
