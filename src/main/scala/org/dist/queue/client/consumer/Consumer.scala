package org.dist.queue.client.consumer

import java.util.concurrent.atomic.AtomicInteger

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api._
import org.dist.queue.client.common.{BrokerPartitionInfo, ClientUtils, PartitionAndLeader}
import org.dist.queue.common.{Logging, TopicAndPartition}
import org.dist.queue.message.KeyedMessage
import org.dist.queue.network.SocketClient
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker

import scala.collection.mutable.HashMap

class Consumer(val clientId:String, bootstrapBroker:InetAddressAndPort, config:Config, val consumerId:Int = Request.OrdinaryConsumerId) extends Logging {
  def findCoordinator() = {
    val findCoordinatorRequest = FindCoordinatorRequest("TestConsumer1", CoordinatorType.GROUP)
    val request = new RequestOrResponse(RequestKeys.FindCoordinatorKey, JsonSerDes.serialize(findCoordinatorRequest), correlationId.getAndIncrement())
    val response = socketClient.sendReceiveTcp(request, bootstrapBroker)
    val coordinatorResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[FindCoordinatorResponse])
    InetAddressAndPort.create(coordinatorResponse.host, coordinatorResponse.port)
  }

  val correlationId = new AtomicInteger(0)

  val socketClient = new SocketClient
  val brokerPartitionInfo = new BrokerPartitionInfo(config,
    bootstrapBroker,
    new HashMap[String, TopicMetadata]())

  def read(topic:String, partitionId:Int) = {
    val correlationIdForRequest: Int = updateBrokerMetadata(topic)

    val partitionInfo: Seq[PartitionAndLeader] = brokerPartitionInfo.getBrokerPartitionInfo(topic, correlationIdForRequest)
    val partitionAndLeader: Seq[PartitionAndLeader] = partitionInfo.filter(_.partitionId == partitionId)
    val leaderBrokerId = partitionAndLeader.head.leaderBrokerIdOpt.get
    val leaderBrokerInfo: Broker = brokerPartitionInfo.getBroker(leaderBrokerId).get
    fetch(topic, partitionId, leaderBrokerInfo, 0)
  }

  private def updateBrokerMetadata(topic: String) = {
    val correlationIdForRequest = correlationId.getAndIncrement()
    val topicMetadata: Seq[TopicMetadata] = new ClientUtils().fetchTopicMetadata(Set(topic), correlationIdForRequest, clientId, bootstrapBroker)
    brokerPartitionInfo.updateInfo(Set(topic), correlationIdForRequest, topicMetadata)
    correlationIdForRequest
  }
  def fetch(topic: String, partitionId: Int, leaderBrokerInfo: Broker, initialOffset: Long): PartitionData = {
    val data = fetch(Map(TopicAndPartition(topic, partitionId) → initialOffset), leaderBrokerInfo)
    data(TopicAndPartition(topic, partitionId))
  }

  def fetch(requestMap:Map[TopicAndPartition, Long], leaderBrokerInfo: Broker): Map[TopicAndPartition, PartitionData] = {
    val mapFunc = (tuple:(TopicAndPartition, Long)) => {
      (TopicAndPartition(tuple._1.topic, tuple._1.partition), PartitionFetchInfo(tuple._2, config.FetchSize))
    }
    val requestInfo = requestMap.map(mapFunc)
    val fetchRequestCorrelationId = correlationId.getAndIncrement()
    val fetchRequest = FetchRequest(fetchRequestCorrelationId,
      clientId,
      consumerId,
      FetchRequest.DefaultMaxWait,
      FetchRequest.DefaultMinBytes,
      requestInfo.toMap)

    val request = new RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(fetchRequest), fetchRequestCorrelationId)
    val response: RequestOrResponse = socketClient.sendReceiveTcp(request, InetAddressAndPort.create(leaderBrokerInfo.host, leaderBrokerInfo.port))

    val fetchResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[FetchResponse])
    fetchResponse.dataAsMap
  }
}
