package org.dist.simplekafka

import com.fasterxml.jackson.core.`type`.TypeReference
import com.google.common.annotations.VisibleForTesting
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener, IZkStateListener, ZkClient}
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.dist.kvstore.JsonSerDes
import org.dist.queue.common.Logging
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.utils.{ZKStringSerializer, ZkUtils}

import scala.jdk.CollectionConverters._


trait ZookeeperClient {
  def shutdown()

  def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas])

  def setPartitionLeaderForTopic(topicName: String, leaderAndReplicas: List[LeaderAndReplicas])

  def getAllBrokerIds(): Set[Int]

  def getAllBrokers(): Set[Broker]

  def getPartitionReplicaLeaderInfo(topicName: String): List[LeaderAndReplicas]

  def getTopics(): List[String]

  def getBrokerInfo(brokerId: Int): Broker

  def getPartitionAssignmentsFor(topicName: String): List[PartitionReplicas]

  def subscribeTopicChangeListener(listener: IZkChildListener): Option[List[String]]

  def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]]

  def subscribeControllerChangeListner(controller: Controller): Unit

  def registerSelf()

  def tryCreatingControllerPath(data: String)
}

case class ControllerExistsException(controllerId: String) extends RuntimeException

private[simplekafka] class ZookeeperClientImpl(config: Config) extends ZookeeperClient {
  val BrokerTopicsPath = "/brokers/topics"
  val BrokerIdsPath = "/brokers/ids"
  val ControllerPath = "/controller"
  val ReplicaLeaderElectionPath = "/topics/replica/leader"


  private val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)
  zkClient.subscribeStateChanges(new SessionExpireListener)

  override def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas]) = {
    val topicsPath = getTopicPath(topicName)
    val topicsData = JsonSerDes.serialize(partitionReplicas)
    createPersistentPath(zkClient, topicsPath, topicsData)
  }

  override def getAllBrokerIds(): Set[Int] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(_.toInt).toSet
  }

  def getAllBrokers(): Set[Broker] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(brokerId => {
      val data: String = zkClient.readData(getBrokerPath(brokerId.toInt))
      JsonSerDes.deserialize(data.getBytes, classOf[Broker])
    }).toSet
  }

  def getBrokerInfo(brokerId: Int): Broker = {
    val data: String = zkClient.readData(getBrokerPath(brokerId))
    JsonSerDes.deserialize(data.getBytes, classOf[Broker])
  }

  def getPartitionReplicaLeaderInfo(topicName: String): List[LeaderAndReplicas] = {
      val leaderAndReplicas: String = zkClient.readData(getReplicaLeaderElectionPath(topicName))
      JsonSerDes.deserialize[List[LeaderAndReplicas]](leaderAndReplicas.getBytes, new TypeReference[List[LeaderAndReplicas]]() {})
  }


  override def registerSelf(): Unit = {
    val broker = Broker(config.brokerId, config.hostName, config.port)
    registerBroker(broker)
  }

  override def getPartitionAssignmentsFor(topicName: String): List[PartitionReplicas] = {
    val partitionAssignments: String = zkClient.readData(getTopicPath(topicName))
    JsonSerDes.deserialize[List[PartitionReplicas]](partitionAssignments.getBytes, new TypeReference[List[PartitionReplicas]]() {})
  }

  def subscribeTopicChangeListener(listener: IZkChildListener): Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BrokerTopicsPath, listener)
    Option(result).map(_.asScala.toList)
  }

  def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BrokerIdsPath, listener)
    Option(result).map(_.asScala.toList)
  }

  //broker/ids/1 {host:10.0.0.1, port:8080}
  @VisibleForTesting
  def registerBroker(broker: Broker) = {
    val brokerData = JsonSerDes.serialize(broker)
    val brokerPath = getBrokerPath(broker.id)
    createEphemeralPath(zkClient, brokerPath, brokerData)
  }

  @VisibleForTesting
  def getAllTopics() = {
    val topics = zkClient.getChildren(BrokerTopicsPath).asScala
    topics.map(topicName => {
      val partitionAssignments: String = zkClient.readData(getTopicPath(topicName))
      val partitionReplicas: List[PartitionReplicas] = JsonSerDes.deserialize[List[PartitionReplicas]](partitionAssignments.getBytes, new TypeReference[List[PartitionReplicas]]() {})
      (topicName, partitionReplicas)
    }).toMap
  }

  def getTopics() = zkClient.getChildren(BrokerTopicsPath).asScala.toList

  def createEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
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

  private def getBrokerPath(id: Int) = {
    BrokerIdsPath + "/" + id
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

  override def tryCreatingControllerPath(controllerId: String): Unit = {
    try {
      createEphemeralPath(zkClient, ControllerPath, controllerId)
    } catch {
      case e: ZkNodeExistsException => {
        val existingControllerId: String = zkClient.readData(ControllerPath)
        throw new ControllerExistsException(existingControllerId)
      }
    }
  }

  override def subscribeControllerChangeListner(controller: Controller): Unit = {
    zkClient.subscribeDataChanges(ControllerPath, new ControllerChangeListener(controller))
  }

  override def shutdown(): Unit = zkClient.close()


  class ControllerChangeListener(controller: Controller) extends IZkDataListener {
    override def handleDataChange(dataPath: String, data: Any): Unit = {
      val existingControllerId:String = zkClient.readData(dataPath)
      controller.setCurrent(existingControllerId.toInt)
    }

    override def handleDataDeleted(dataPath: String): Unit = {
      controller.elect()
      if (controller.currentLeader.equals(controller.brokerId)) {
        controller.electNewLeaderForPartition();
      }
    }
  }

  def getReplicaLeaderElectionPath(topicName: String) = {
    ReplicaLeaderElectionPath + "/" + topicName
  }

  override def setPartitionLeaderForTopic(topicName: String, leaderAndReplicas: List[LeaderAndReplicas]): Unit = {

    val leaderReplicaSerializer = JsonSerDes.serialize(leaderAndReplicas)
    val path = getReplicaLeaderElectionPath(topicName);

    try {
      ZkUtils.updatePersistentPath(zkClient,path, leaderReplicaSerializer)
    } catch {
      case e: Throwable => {
        println("Exception while writing data to partition leader data" + e)
      }
    }
  }
}
