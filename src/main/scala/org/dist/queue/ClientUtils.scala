package org.dist.queue

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api.{RequestKeys, RequestOrResponse, TopicMetadata, TopicMetadataRequest, TopicMetadataResponse}
import org.dist.queue.network.SocketClient

class ClientUtils {
  val socketClient = new SocketClient


  def fetchTopicMetadata(topics: Set[String], correlationId: Int, clientId:String, bootstrapBroker:InetAddressAndPort): Seq[TopicMetadata] = {
    val topicMetadataRequest = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, correlationId, clientId, topics.toSeq)
    val response = socketClient.sendReceiveTcp(new RequestOrResponse(RequestKeys.MetadataKey, JsonSerDes.serialize(topicMetadataRequest), correlationId), bootstrapBroker)
    val topicMetadataResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[TopicMetadataResponse])
    topicMetadataResponse.topicsMetadata
  }
}
