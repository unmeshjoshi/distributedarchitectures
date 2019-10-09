package org.dist.queue.api

import java.util

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.message.ByteBufferMessageSet

import scala.jdk.CollectionConverters._
import scala.collection.mutable

object ProduceRequest {
  val CurrentVersion = 0.shortValue

  def apply(correlationId: Int,
            clientId: String,
            requiredAcks: Short,
            ackTimeoutMs: Int,
            data: Map[TopicAndPartition, ByteBufferMessageSet]) = {

    new ProduceRequest(ProduceRequest.CurrentVersion, correlationId, clientId, requiredAcks, ackTimeoutMs, convertToStringKeyMap(data.toMap))
  }

  def convertToStringKeyMap(data: Map[TopicAndPartition, ByteBufferMessageSet]):mutable.Map[String, ByteBufferMessageSet] = {
    val map = new util.HashMap[String, ByteBufferMessageSet]()
    val keys = data.keySet
    for(key <- keys) {
      val strKey = s"${key.topic}:${key.partition}"
      map.put(strKey, data(key))
    }
    map.asScala
  }
}

case class ProduceRequest(versionId: Short = ProduceRequest.CurrentVersion,
                          correlationId: Int,
                          clientId: String,
                          requiredAcks: Short,
                          ackTimeoutMs: Int,
                          data: mutable.Map[String, ByteBufferMessageSet]) {


  /**
   * Partitions the data into a map of maps (one for each topic).
   */
  private def dataGroupedByTopic = dataAsMap.groupBy(_._1.topic)
  private def mapFunc = (r:(TopicAndPartition, ByteBufferMessageSet)) => {
    r._1 -> r._2.sizeInBytes
  }
  def topicPartitionMessageSizeMap = dataAsMap.map(mapFunc).toMap


  def numPartitions = data.size


  def emptyData() = {
    //FIXME: Fix this
    //data.clear
  }

  def dataAsMap = {


    if (data == null) {
      Map[TopicAndPartition, ByteBufferMessageSet]()
    } else {
      val map = new util.HashMap[TopicAndPartition, ByteBufferMessageSet]()
      val set = data.keySet
      for (key <- set) {
        val splits: Array[String] = key.split(":")
        val tuple = TopicAndPartition(splits(0), splits(1).toInt)
        map.put(tuple, data(key))
      }
      map.asScala.toMap
    }
  }


}