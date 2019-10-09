package org.dist.simplekafka

import com.fasterxml.jackson.core.`type`.TypeReference
import com.google.common.annotations.VisibleForTesting
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.I0Itec.zkclient.{IZkChildListener, IZkStateListener, ZkClient}
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.dist.kvstore.JsonSerDes
import org.dist.queue.common.Logging
import org.dist.queue.server.Config
import org.dist.queue.utils.{ZKStringSerializer, ZkUtils}
import org.dist.queue.utils.ZkUtils.{Broker, BrokerTopicsPath, createParentPath, getTopicPath}

import scala.jdk.CollectionConverters._


trait ZookeeperClient {
  def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas])
  def getAllBrokerIds():List[Int]
  def getPartitionAssignmentsFor(topicName:String):List[PartitionReplicas]
  def subscribeTopicChangeListener(listener: IZkChildListener):Option[List[String]]
  def registerSelf()
}

class ZookeeperClientImpl(config:Config) extends ZookeeperClient {
  private val BrokerTopicsPath = "/brokers/topics"
  private val BrokerIdsPath = "/brokers/ids"

  private val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)
  zkClient.subscribeStateChanges(new SessionExpireListener)

  override def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas]) = {
    val topicsPath = getTopicPath(topicName)
    val topicsData = JsonSerDes.serialize(partitionReplicas)
    createPersistentPath(zkClient, topicsPath, topicsData)
  }

  override def getAllBrokerIds() = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(_.toInt).toList
  }

  override def registerSelf(): Unit = {
    val broker = Broker(config.brokerId, config.hostName, config.port)
    registerBroker(broker)
  }

  override def getPartitionAssignmentsFor(topicName: String): List[PartitionReplicas] = {
    val partitionAssignments:String = zkClient.readData(getTopicPath(topicName))
    JsonSerDes.deserialize[List[PartitionReplicas]](partitionAssignments.getBytes, new TypeReference[List[PartitionReplicas]](){})
  }

  def subscribeTopicChangeListener(listener: IZkChildListener):Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BrokerTopicsPath, listener)
    Option(result).map(_.asScala.toList)
  }

  @VisibleForTesting
  def registerBroker(broker: Broker) = {
    val brokerData = JsonSerDes.serialize(broker)
    val brokerPath = BrokerIdsPath + "/" + broker.id
    createEphemeralPath(zkClient, brokerPath, brokerData)
  }

  @VisibleForTesting
  def getAllTopics() = {
    val topics = zkClient.getChildren(BrokerTopicsPath).asScala
    topics.map(topicName ⇒ {
      val partitionAssignments:String = zkClient.readData(getTopicPath(topicName))
      val partitionReplicas:List[PartitionReplicas] = JsonSerDes.deserialize[List[PartitionReplicas]](partitionAssignments.getBytes, new TypeReference[List[PartitionReplicas]](){})
      (topicName, partitionReplicas)
    }).toMap
  }

  private def createEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.createEphemeral(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
    }
  }

  def createPersistentPath(client: ZkClient, path: String, data: String = ""): Unit = {
    try {
      client.createPersistent(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createPersistent(path, data)
      }
    }
  }

  private def getTopicPath(topicName: String) = {
    BrokerTopicsPath + "/" + topicName
  }

  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  class SessionExpireListener() extends IZkStateListener with Logging {
    @throws(classOf[Exception])
    def handleStateChanged(state: KeeperState) {
      // do nothing, since zkclient will do reconnect for us.
    }

    @throws(classOf[Exception])
    def handleNewSession() {
      info("re-registering broker info in ZK for broker " + config.brokerId)
      registerSelf()
      info("done re-registering broker")
      info("Subscribing to %s path to watch for new topics".format(ZkUtils.BrokerTopicsPath))
    }

    override def handleSessionEstablishmentError(error: Throwable): Unit = {
      debug(error)
    }
  }
}
