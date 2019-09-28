package org.dist.queue.api

import java.nio.ByteBuffer
import java.util

import org.dist.kvstore.JsonSerDes
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{ByteBufferMessageSet, ErrorMapping, PartitionStateInfo, TopicAndPartition}

import scala.collection.JavaConverters._
import scala.collection.Map


object Request {
  val OrdinaryConsumerId: Int = -1
  val DebuggingConsumerId: Int = -2
}


object RequestKeys {
  def deserializerForKey(requestId: Short)(buffer:ByteBuffer) = ???

  val ProduceKey: Short = 0
  val FetchKey: Short = 1
  val OffsetsKey: Short = 2
  val MetadataKey: Short = 3
  val LeaderAndIsrKey: Short = 4
  val StopReplicaKey: Short = 5
  val UpdateMetadataKey: Short = 6
  val ControlledShutdownKey: Short = 7
}

case class RequestOrResponse(val requestId: Short, val messageBodyJson: String, val correlationId: Int) {
  def serialize(): String = {
    JsonSerDes.serialize(this)
  }
}


object ProducerRequest {
  val CurrentVersion = 0.shortValue

  def apply(correlationId: Int,
            clientId: String,
            requiredAcks: Short,
            ackTimeoutMs: Int,
            data: collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]) = {

    new ProducerRequest(ProducerRequest.CurrentVersion, correlationId, clientId, requiredAcks, ackTimeoutMs, convertToStringKeyMap(data))
  }

  def convertToStringKeyMap(data: Map[TopicAndPartition, ByteBufferMessageSet]):collection.mutable.Map[String, ByteBufferMessageSet] = {
    val map = new util.HashMap[String, ByteBufferMessageSet]()
    val keys = data.keySet
    for(key <- keys) {
      val strKey = s"${key.topic}:${key.partition}"
      map.put(strKey, data(key))
    }
    map.asScala
  }
}

case class ProducerRequest(versionId: Short = ProducerRequest.CurrentVersion,
                           correlationId: Int,
                           clientId: String,
                           requiredAcks: Short,
                           ackTimeoutMs: Int,
                           data: collection.mutable.Map[String, ByteBufferMessageSet]) {
  def dataAsMap = {


    if (data == null) {
      Map[TopicAndPartition, ByteBufferMessageSet]()
    } else {
      val map = new util.HashMap[TopicAndPartition, ByteBufferMessageSet]()
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

case class TopicMetadataRequest(val versionId: Short,
                                val correlationId: Int,
                                val clientId: String,
                                val topics: Seq[String])

case class TopicMetadataResponse(topicsMetadata: Seq[TopicMetadata],
                                 val correlationId: Int) {

}

case class TopicMetadata(topic: String, partitionsMetadata: Seq[PartitionMetadata], errorCode: Short = ErrorMapping.NoError)

case class PartitionMetadata(partitionId: Int,
                             val leader: Option[Broker],
                             replicas: Seq[Broker],
                             isr: Seq[Broker] = Seq.empty,
                             errorCode: Short = ErrorMapping.NoError)


object LeaderAndIsrRequest {
  val CurrentVersion = 0.shortValue
  val IsInit: Boolean = true
  val NotInit: Boolean = false
  val DefaultAckTimeout: Int = 1000

  def apply(partitionStateInfos: Map[(String, Int), PartitionStateInfo],
            leaders: Set[Broker], controllerId: Int,
           controllerEpoch: Int, correlationId: Int, clientId: String) = {

    new LeaderAndIsrRequest(LeaderAndIsrRequest.CurrentVersion, correlationId, clientId,
      controllerId, controllerEpoch, convertToStringKeyMap(partitionStateInfos), leaders)
  }


  def convertToStringKeyMap(partitionStateInfos: Map[(String, Int), PartitionStateInfo]):Map[String, PartitionStateInfo] = {
    val map = new util.HashMap[String, PartitionStateInfo]()
    val keys = partitionStateInfos.keySet
    for(key <- keys) {
      val strKey = s"${key._1}:${key._2}"
      map.put(strKey, partitionStateInfos(key))
    }
    map.asScala.toMap
  }

}



case class LeaderAndIsrRequest(versionId: Short,
                               val correlationId: Int,
                               clientId: String,
                               controllerId: Int,
                               controllerEpoch: Int,
                               partitionStateInfos: Map[String, PartitionStateInfo], // cant deserialize map with tuple (String, Int) key so adding helper method
                               leaders: Set[Broker]) {




  def partitionStateInfoMap = {


    if (partitionStateInfos == null) {
      Map[(String, Int), PartitionStateInfo]()
    } else {
      val map = new util.HashMap[(String, Int), PartitionStateInfo]()
      val set = partitionStateInfos.keySet
      for (key ← set) {
        val splits: Array[String] = key.split(":")
        val tuple = (splits(0), splits(1).toInt)
        map.put(tuple, partitionStateInfos(key))
      }
      map.asScala.toMap
    }
  }
}


object UpdateMetadataRequest {
  val CurrentVersion = 0.shortValue
  val IsInit: Boolean = true
  val NotInit: Boolean = false
  val DefaultAckTimeout: Int = 1000

  def apply(controllerId: Int, controllerEpoch: Int, correlationId: Int, clientId: String,
           partitionStateInfos: Map[TopicAndPartition, PartitionStateInfo], aliveBrokers: Set[Broker]) = {
    new UpdateMetadataRequest(UpdateMetadataRequest.CurrentVersion, correlationId, clientId,
      controllerId, controllerEpoch, convertToStringKeyMap(partitionStateInfos), aliveBrokers)
  }

  def convertToStringKeyMap(partitionStateInfos: Map[TopicAndPartition, PartitionStateInfo]):Map[String, PartitionStateInfo] = {
    val map = new util.HashMap[String, PartitionStateInfo]()
    val keys = partitionStateInfos.keySet
    for(key <- keys) {
      val strKey = s"${key.topic}:${key.partition}"
      map.put(strKey, partitionStateInfos(key))
    }
    map.asScala.toMap
  }


}

case class UpdateMetadataRequest (versionId: Short,
                                  val correlationId: Int,
                                  clientId: String,
                                  controllerId: Int,
                                  controllerEpoch: Int,
                                  partitionStateInfos: Map[String, PartitionStateInfo],
                                  aliveBrokers: Set[Broker]) {

  def partitionStateInfoMap = {
    if (partitionStateInfos == null) {
      Map[TopicAndPartition, PartitionStateInfo]()
    } else {
      val map = new util.HashMap[TopicAndPartition, PartitionStateInfo]()
      val set = partitionStateInfos.keySet
      for (key ← set) {
        val splits: Array[String] = key.split(":")
        val topicPartition = TopicAndPartition(splits(0), splits(1).toInt)
        map.put(topicPartition, partitionStateInfos(key))
      }
      map.asScala.toMap
    }

  }
}


case class LeaderAndIsrResponse(val correlationId: Int,
                                responseMap: Map[(String, Int), Short],
                                errorCode: Short = 0)

object LeaderAndIsrResponse {
  def readFrom(buffer: ByteBuffer): RequestOrResponse = {
    RequestOrResponse(1, "", 1)
  }
}


case class UpdateMetadataResponse(val correlationId: Int,
                                  errorCode: Short = 0)

object UpdateMetadataResponse {
  def readFrom(buffer: ByteBuffer): RequestOrResponse = {
    RequestOrResponse(1, "", 1)
  }
}
