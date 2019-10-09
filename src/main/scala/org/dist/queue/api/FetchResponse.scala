package org.dist.queue.api

import java.util

import org.dist.queue.common.{ErrorMapping, TopicAndPartition}
import org.dist.queue.message.{KeyedMessage, MessageSet}

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Map

object FetchResponsePartitionData {
  val headerSize =
    2 + /* error code */
      8 + /* high watermark */
      4 /* messageSetSize */
}
case class FetchResponsePartitionData(error: Short = ErrorMapping.NoError, hw: Long = -1L, messages: MessageSet) {

  val sizeInBytes = FetchResponsePartitionData.headerSize + messages.sizeInBytes

  def this(messages: MessageSet) = this(ErrorMapping.NoError, -1L, messages)

}

object FetchResponse {

  val headerSize =
    4 + /* correlationId */
      4 /* topic count */

  def apply(correlationId: Int,
                          data: Map[TopicAndPartition, PartitionData]) = {

    new FetchResponse(FetchRequest.CurrentVersion, correlationId, convertToStringKeyMap(data))
  }

  def convertToStringKeyMap(data: Map[TopicAndPartition, PartitionData]):Map[String, PartitionData] = {
    val map = new util.HashMap[String, PartitionData]()
    for(key <- data.keySet) {
      val strKey = s"${key.topic}:${key.partition}"
      map.put(strKey, data(key))
    }
    map.asScala.toMap
  }
}

case class PartitionData(messages:List[KeyedMessage[String, String]], lastOffset:Long)

case class FetchResponse(versionId:Int, correlationId: Int,
                         data: Map[String, PartitionData])  {


  def dataAsMap = {
    if (data == null) {
      Map[TopicAndPartition, PartitionData]()
    } else {
      val map = new util.HashMap[TopicAndPartition, PartitionData]()
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
