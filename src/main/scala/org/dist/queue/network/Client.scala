package org.dist.queue.network

import java.util.concurrent.atomic.AtomicInteger

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.Config
import org.dist.queue.api.{ProducerRequest, RequestKeys, RequestOrResponse, TopicMetadataRequest, TopicMetadataResponse}

import scala.collection.Set

class Client(bootstrapBroker:InetAddressAndPort, config:Config) {
  val correlationId = new AtomicInteger(0)
  val clientId = "client1"
  val socketClient = new SocketClient


  def fetchTopicMetadata(topics: Set[String]): Unit = {
    val correlationIdForRequest = correlationId.getAndIncrement()
    val topicMetadataRequest = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, correlationIdForRequest, clientId, topics.toSeq)
    val response = socketClient.sendReceiveTcp(new RequestOrResponse(RequestKeys.MetadataKey, JsonSerDes.serialize(topicMetadataRequest), correlationIdForRequest), bootstrapBroker)
     val topicMetadataResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[TopicMetadataResponse])
    print(topicMetadataResponse.topicsMetadata)
  }
}
