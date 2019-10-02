package org.dist.queue.client.consumer

import java.util.concurrent.atomic.AtomicInteger

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api._
import org.dist.queue.client.common.{BrokerPartitionInfo, ClientUtils, PartitionAndLeader}
import org.dist.queue.network.SocketClient
import org.dist.queue._
import org.dist.queue.message.KeyedMessage

import scala.collection.mutable.HashMap

class Consumer(bootstrapBroker:InetAddressAndPort, config:Config) extends Logging {
  val correlationId = new AtomicInteger(0)
  val clientId = "Consumer1"
  val socketClient = new SocketClient
  val brokerPartitionInfo = new BrokerPartitionInfo(config,
    bootstrapBroker,
    new HashMap[String, TopicMetadata]())

  def read(topic:String, partitionId:Int) = {
    val correlationIdForRequest = correlationId.getAndIncrement()
    val topicMetadata = new ClientUtils().fetchTopicMetadata(Set(topic), correlationIdForRequest, clientId, bootstrapBroker)
    brokerPartitionInfo.updateInfo(Set(topic), correlationIdForRequest, topicMetadata)
    val partitionInfo: Seq[PartitionAndLeader] = brokerPartitionInfo.getBrokerPartitionInfo(topic, correlationIdForRequest)
    val partitionAndLeader: Seq[PartitionAndLeader] = partitionInfo.filter(_.partitionId == partitionId)
    val leaderBrokerId = partitionAndLeader.head.leaderBrokerIdOpt.get

    val leaderBrokerInfo = brokerPartitionInfo.getBroker(leaderBrokerId).get
    val requestInfo = new collection.mutable.HashMap[TopicAndPartition, PartitionFetchInfo]
    requestInfo.put(TopicAndPartition(topic, partitionId), PartitionFetchInfo(0, config.FetchSize))

    val fetchRequestCorrelationId = correlationId.getAndIncrement()
    val fetchRequest = FetchRequest(fetchRequestCorrelationId,
      clientId,
      Request.OrdinaryConsumerId,
      FetchRequest.DefaultMaxWait,
      FetchRequest.DefaultMinBytes,
      requestInfo.toMap)

    val request = new RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(fetchRequest), fetchRequestCorrelationId)
    val response: RequestOrResponse = socketClient.sendReceiveTcp(request, InetAddressAndPort.create(leaderBrokerInfo.host, leaderBrokerInfo.port))

    val fetchResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[FetchResponse])
    val data: Seq[KeyedMessage[String, String]] = fetchResponse.dataAsMap(TopicAndPartition(topic, partitionId))
    data.toList
  }
}
