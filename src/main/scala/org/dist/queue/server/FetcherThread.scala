package org.dist.queue.server

import java.util.concurrent.locks.ReentrantLock

import org.apache.zookeeper.ZKUtil
import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.client.consumer.Consumer
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils

import scala.collection.mutable

class FetcherThread(leaderBroker:ZkUtils.Broker, config:Config) extends Thread {
  private val partitionMap = new mutable.HashMap[TopicAndPartition, Long]
  private val partitionMapLock = new ReentrantLock
  private val partitionMapCond = partitionMapLock.newCondition()
  val consumer = new Consumer(InetAddressAndPort.create(leaderBroker.host, leaderBroker.port), config)

  def addPartition(topic: String, partitionId: Int, initialOffset: Long) = partitionMap.put(TopicAndPartition(topic, partitionId), initialOffset)

  def doWork() = {
    partitionMap.foreach((topicPartitionAndOffset: (TopicAndPartition, Long))⇒{
      val keyedMessages = consumer.fetch(topicPartitionAndOffset._1.topic, topicPartitionAndOffset._1.partition, leaderBroker, topicPartitionAndOffset._2)
    })

  }
}
