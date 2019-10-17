package org.dist.simplekafka

import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener, ZkClient}
import org.dist.queue.utils.ZkUtils.Broker
import org.scalatest.FunSuite

class TestZookeeperClient(brokerIds:List[Int]) extends ZookeeperClient {
  var topicName:String = null
  var partitionReplicas = Set[PartitionReplicas]()
  var topicChangeListner:IZkChildListener = null

  override def getAllBrokerIds(): Set[Int] = Set(0, 1, 2)

  override def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas]): Unit = {
    this.topicName = topicName
    this.partitionReplicas = partitionReplicas
  }

  override def registerSelf(): Unit = {}

  override def getPartitionAssignmentsFor(topicName: String): List[PartitionReplicas] = List()

  override def subscribeTopicChangeListener(listener: IZkChildListener): Option[List[String]] = {
    topicChangeListner = listener
    None
  }

  def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]] = {
    None
  }

  override def tryCreatingControllerPath(data: String): Unit = {}
  def getAllBrokers(): Set[Broker] = Set()
  def getBrokerInfo(brokerId: Int): Broker = Broker(1, "", 0)

  override def shutdown(): Unit = ???

  override def subscribeControllerChangeListner(controller: Controller): Unit = ???
}

class CreateTopicCommandTest extends FunSuite {
  test("should assign set of replicas for partitions of topic") {
    val brokerIds = List(0, 1, 2)
    val zookeeperClient = new TestZookeeperClient(brokerIds)
    val createCommandTest = new CreateTopicCommand(zookeeperClient)
    val noOfPartitions = 3
    val replicationFactor = 2
    createCommandTest.createTopic("topic1", noOfPartitions, replicationFactor)
    assert(zookeeperClient.topicName == "topic1")
    assert(zookeeperClient.partitionReplicas.size == noOfPartitions)
    zookeeperClient.partitionReplicas.map(p => p.brokerIds).foreach(_.size == replicationFactor)
  }

  test("should assign partitions assigned to ") {
    val brokerIds = List(0, 1, 2)
    val zookeeperClient = new TestZookeeperClient(brokerIds)
    val createCommandTest = new CreateTopicCommand(zookeeperClient)
    val noOfPartitions = 3
    val replicationFactor = 2
    createCommandTest.createTopic("topic1", noOfPartitions, replicationFactor)
    assert(zookeeperClient.topicName == "topic1")
    assert(zookeeperClient.partitionReplicas.size == noOfPartitions)
    zookeeperClient.partitionReplicas.map(p => p.brokerIds).foreach(_.size == replicationFactor)
  }
}
