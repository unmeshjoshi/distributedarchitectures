package org.dist.queue.client.common

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.api.TopicMetadata
import org.dist.queue.common.{ErrorMapping, KafkaException, Logging}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker

import scala.collection.mutable.HashMap

class BrokerPartitionInfo(config:Config,
                          bootStrapBroker:InetAddressAndPort,
                          topicPartitionInfo: HashMap[String, TopicMetadata]) extends Logging {

  private val brokers = new collection.mutable.HashMap[Int, Broker]()

  def updateProducer(topicMetadata: Seq[TopicMetadata]) =  {
    topicMetadata.foreach(tmd => {
      tmd.partitionsMetadata.foreach(pmd => {
        if (pmd.leader.isDefined)
          brokers.put(pmd.leader.get.id, pmd.leader.get)
      })
    })
  }

  def getBroker(brokerId:Int) = {
    brokers.get(brokerId)
  }


  /**
   * Return a sequence of (brokerId, numPartitions).
   * @param topic the topic for which this information is to be returned
   * @return a sequence of (brokerId, numPartitions). Returns a zero-length
   * sequence if no brokers are available.
   */
  def getBrokerPartitionInfo(topic: String, correlationId: Int): Seq[PartitionAndLeader] = {
    debug("Getting broker partition info for topic %s".format(topic))
    // check if the cache has metadata for this topic
    val topicMetadata = topicPartitionInfo.get(topic)
    val metadata: TopicMetadata =
      topicMetadata match {
        case Some(m) => m
        case None =>
          // refresh the topic metadata cache
          //FIXME: Implement following method to update topic metadata
          //          updateInfo(Set(topic), correlationId)
          val topicMetadata = topicPartitionInfo.get(topic)
          topicMetadata match {
            case Some(m) => m
            case None => throw new KafkaException("Failed to fetch topic metadata for topic: " + topic)
          }
      }
    val partitionMetadata = metadata.partitionsMetadata
    if(partitionMetadata.size == 0) {
      if(metadata.errorCode != ErrorMapping.NoError) {
        throw new KafkaException("" + ErrorMapping.UnknownCode)
      } else {
        throw new KafkaException("Topic metadata %s has empty partition metadata and no error code".format(metadata))
      }
    }
    partitionMetadata.map { m =>
      m.leader match {
        case Some(leader) =>
          debug("Partition [%s,%d] has leader %d".format(topic, m.partitionId, leader.id))
          new PartitionAndLeader(topic, m.partitionId, Some(leader.id))
        case None =>
          debug("Partition [%s,%d] does not have a leader yet".format(topic, m.partitionId))
          new PartitionAndLeader(topic, m.partitionId, None)
      }
    }.sortWith((s, t) => s.partitionId < t.partitionId)
  }

  /**
   * It updates the cache by issuing a get topic metadata request to a random broker.
   * @param topics the topics for which the metadata is to be fetched
   */
  def updateInfo(topics: Set[String], correlationId: Int, topicsMetadata:Seq[TopicMetadata]) {
    updateProducer(topicsMetadata)
    // throw partition specific exception
    topicsMetadata.foreach(tmd =>{
      trace("Metadata for topic %s is %s".format(tmd.topic, tmd))
      if(tmd.errorCode == ErrorMapping.NoError) {
        topicPartitionInfo.put(tmd.topic, tmd)

      } else
        warn("Error while fetching metadata [%s] for topic [%s]: %s ".format(tmd, tmd.topic, ErrorMapping.UnknownCode))
      tmd.partitionsMetadata.foreach(pmd =>{
        if (pmd.errorCode != ErrorMapping.NoError && pmd.errorCode == ErrorMapping.LeaderNotAvailableCode) {
          warn("Error while fetching metadata %s for topic partition [%s,%d]: [%s]".format(pmd, tmd.topic, pmd.partitionId,
            ErrorMapping.UnknownCode))
        } // any other error code (e.g. ReplicaNotAvailable) can be ignored since the producer does not need to access the replica and isr metadata
      })
    })
  }
}
