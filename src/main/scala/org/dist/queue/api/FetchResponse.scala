package org.dist.queue.api

import java.util

import org.dist.queue.common.{ErrorMapping, TopicAndPartition}
import org.dist.queue.message.{KeyedMessage, MessageSet}

import scala.collection.JavaConverters._
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
                          data: Map[TopicAndPartition, List[KeyedMessage[String, String]]]) = {

    new FetchResponse(FetchRequest.CurrentVersion, correlationId, convertToStringKeyMap(data))
  }

  def convertToStringKeyMap(data: Map[TopicAndPartition, List[KeyedMessage[String, String]]]):Map[String, List[KeyedMessage[String, String]]] = {
    val map = new util.HashMap[String, List[KeyedMessage[String, String]]]()
    for(key <- data.keySet) {
      val strKey = s"${key.topic}:${key.partition}"
      map.put(strKey, data(key))
    }
    map.asScala.toMap
  }
}

case class FetchResponse(versionId:Int, correlationId: Int,
                         data: Map[String, List[KeyedMessage[String, String]]])  {


  def dataAsMap = {
    if (data == null) {
      Map[TopicAndPartition, List[KeyedMessage[String, String]]]()
    } else {
      val map = new util.HashMap[TopicAndPartition, List[KeyedMessage[String, String]]]()
      val set = data.keySet
      for (key ← set) {
        val splits: Array[String] = key.split(":")
        val tuple = TopicAndPartition(splits(0), splits(1).toInt)
        map.put(tuple, data(key))
      }
      map.asScala.toMap
    }
  }
}
