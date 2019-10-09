package org.dist.queue.server

import java.util.concurrent.locks.ReentrantLock

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.api.{PartitionData, Request}
import org.dist.queue.client.consumer.Consumer
import org.dist.queue.common.{KafkaStorageException, Logging, TopicAndPartition}
import org.dist.queue.message.{ByteBufferMessageSet, KeyedMessage, Message, NoCompressionCodec}
import org.dist.queue.utils.{Utils, ZkUtils}

import scala.collection.mutable

class FetcherThread(fetcherId: String, replicaMgr: ReplicaManager, leaderBroker: ZkUtils.Broker, config: Config) extends Thread with Logging {
  val consumer = new Consumer(fetcherId, InetAddressAndPort.create(leaderBroker.host, leaderBroker.port), config, Request.FollowerReplicaFetcherId)
  private val partitionMap = new mutable.HashMap[TopicAndPartition, Long]
  private val partitionMapLock = new ReentrantLock

  def addPartition(topic: String, partitionId: Int, initialOffset: Long) = partitionMap.put(TopicAndPartition(topic, partitionId), initialOffset)

  override def run(): Unit = {
    while (true) {
      Thread.sleep(200) //fetch every 1 second
      doWork()
    }
  }

  def doWork() = {
    Utils.inLock(partitionMapLock) {
      try {
        val partitionFetchData: Map[TopicAndPartition, PartitionData] = consumer.fetch(partitionMap.toMap, leaderBroker)
        partitionFetchData.foreach(topicPartitionAndPartitionData => {

          val topicAndPartition = topicPartitionAndPartitionData._1
          val partitionData = topicPartitionAndPartitionData._2
          val fetchedMessages: Seq[KeyedMessage[String, String]] = partitionData.messages
          val serializedMessage: Seq[Message] = fetchedMessages.map(m => serializeMessage(m)).map(km => km.message)


          val replica = replicaMgr.getReplica(topicAndPartition.topic, topicAndPartition.partition).get

          val currentOffset = partitionMap(topicAndPartition)

          val messageSet = new ByteBufferMessageSet(NoCompressionCodec, serializedMessage)

          if (currentOffset != replica.logEndOffset) {
            val e = replica.logEndOffset
            throw new RuntimeException("Offset mismatch: fetched offset = %d, log end offset = %d.".format(currentOffset, replica.logEndOffset))
          }

          trace("Follower %d has replica log end offset %d for partition %s. Received %d messages and leader hw %d"
            .format(replica.brokerId, replica.logEndOffset, topicAndPartition, messageSet.sizeInBytes, partitionData.lastOffset))

          replica.log.get.append(messageSet, true)

          partitionMap.put(topicAndPartition, replica.logEndOffset)

          trace("Follower %d has replica log end offset %d after appending %d bytes of messages for partition %s"
            .format(replica.brokerId, replica.logEndOffset, messageSet.sizeInBytes, topicAndPartition))
          val followerHighWatermark = replica.logEndOffset.min(partitionData.lastOffset)
          replica.highWatermark = followerHighWatermark
          trace("Follower %d set replica highwatermark for partition [%s,%d] to %d"
            .format(replica.brokerId, topicAndPartition.topic, topicAndPartition.partition, followerHighWatermark))
        })
      } catch {
        case e: KafkaStorageException =>
          fatal("Disk error while replicating data.", e)
          Runtime.getRuntime.halt(1)
      }
    }
  }


  private def serializeMessage(keyedMessage: KeyedMessage[String, String]) = {
    new KeyedMessage[String, Message](keyedMessage.topic,
      new Message(StringEncoder.toBytes(keyedMessage.message), StringEncoder.toBytes(keyedMessage.key), NoCompressionCodec))
  }

  object StringEncoder {
    def toBytes(key: String) = key.getBytes("UTF8")
  }

}
