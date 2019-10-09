package org.dist.queue

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.controller.{Controller, TopicChangeListener}
import org.dist.queue.network.SocketServer
import org.dist.queue.server.Config
import org.dist.queue.utils.{AdminUtils, ZkUtils}
import org.scalatest.FunSuite

import scala.collection.{Map, mutable}

class AdminUtilsTest extends ZookeeperTestHarness {

  test("should create assign replicas to topic partitions") {
    val value: collection.Map[Int, collection.Seq[Int]] = AdminUtils.assignReplicasToBrokers(Seq(0, 1), 1, 1)
    assert(value(0) == List(1))
  }

  test("should spread replicas on other brokers") {
    val partitionAssignments: collection.Map[Int, collection.Seq[Int]] = AdminUtils.assignReplicasToBrokers(Seq( 0, 1, 3, 4), 7, 3)
    val expected = Map(0 -> List(0, 3, 1),
      1 -> List(3, 1, 5),
      2 -> List(1, 5, 4),
      3 -> List(5, 4, 2),
      4 -> List(4, 2, 0),
      5 -> List(2, 0, 3),
      6 -> List(0, 4, 2))


    val config = Config(1, "localhost",8888, zkConnect, List(TestUtils.tempDir().getAbsolutePath()))
    val socketServer = new SocketServer(config.brokerId,
      config.hostName,
      config.port,
      config.numNetworkThreads,
      config.queuedMaxRequests,
      config.socketSendBufferBytes,
      config.socketReceiveBufferBytes,
      config.socketRequestMaxBytes)
    val controller = new Controller(config, zkClient, socketServer)

    zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, new TopicChangeListener(controller, new AtomicBoolean(true)))

    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK("topic1",
      partitionAssignments, zkClient)

    val value: mutable.Map[TopicAndPartition, collection.Seq[Int]] = ZkUtils.getReplicaAssignmentForTopics(zkClient, Seq("topic1"))
    val replicas: collection.Seq[Int] = value(TopicAndPartition("topic1", 0))
    assert(partitionAssignments(0) == replicas)

  }

  test("should register partition assignments in zookeeper") {
    val assignments: collection.Map[Int, collection.Seq[Int]] = AdminUtils.assignReplicasToBrokers(Seq(0, 1), 1, 1)

    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK("topic1",
      assignments, zkClient)

    val storedAssignments: collection.Seq[Int] = ZkUtils.getReplicasForPartition(zkClient, "topic1", 0)

    assert(assignments(0) == storedAssignments)
  }
}
