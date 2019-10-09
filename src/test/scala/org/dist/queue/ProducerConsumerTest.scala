package org.dist.queue

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.admin.CreateTopicCommand
import org.dist.queue.client.common.PartitionAndLeader
import org.dist.queue.client.consumer.Consumer
import org.dist.queue.client.producer.{DefaultPartitioner, Producer}
import org.dist.queue.common.{Topic, TopicAndPartition}
import org.dist.queue.controller.PartitionStateInfo
import org.dist.queue.message.KeyedMessage
import org.dist.queue.server.{Config, Server}
import org.dist.queue.utils.ZkUtils

class ProducerConsumerTest extends ZookeeperTestHarness {
  test("should produce and consume messages from different partitions") {
    val brokerId1 = 0
    val brokerId2 = 1
    val brokerId3 = 2

    val config1 = Config(brokerId1, TestUtils.hostName(), TestUtils.choosePort(), TestZKUtils.zookeeperConnect, List(TestUtils.tempDir().getAbsolutePath))
    val server1 = new Server(config1)
    server1.startup()

    val config2 = Config(brokerId2, TestUtils.hostName(), TestUtils.choosePort(), TestZKUtils.zookeeperConnect, List(TestUtils.tempDir().getAbsolutePath))
    val server2 = new Server(config2)
    server2.startup()

    val config3 = Config(brokerId3, TestUtils.hostName(), TestUtils.choosePort(), TestZKUtils.zookeeperConnect, List(TestUtils.tempDir().getAbsolutePath))
    val server3 = new Server(config3)
    server3.startup()

    val brokers = Map(brokerId1 → server1, brokerId2 → server2, brokerId3 → server3)

    assertBrokersRegisteredWithZookeeper(config1, config2, config3)


    val topic = "topic1"

    //Create internal topic explicitly. THis can be created internally in Kafka on first request.
    val groupMetadataNumPartitions = Topic.groupMetadataTopicPartitionCount
    CreateTopicCommand.createTopic(zkClient, Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataNumPartitions, 2)

    TestUtils.waitUntilTrue(() => {
      leaderForAllPartitions(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataNumPartitions, List(server1, server2, server3))

    }, "Waiting till metadata for group metadata topic is propagated to all servers", 2000)

    val numPartitions = 3
    CreateTopicCommand.createTopic(zkClient, topic, numPartitions, 3)

    TestUtils.waitUntilTrue(() => {
      leaderForAllPartitions(topic, numPartitions, List(server1, server2, server3))

    }, s"Waiting till topic ${topic} metadata to propagate to all the servers", 2000)

    val bootstrapBroker = InetAddressAndPort.create(config1.hostName, config1.port)

    val messages = List(KeyedMessage(topic, "AaAa", "test message"),
      KeyedMessage(topic, "BBBB", "test message1"),
      KeyedMessage(topic, "AaBB", "test message2"))

    val producer = produceMessages(bootstrapBroker, config1, messages)

    var allConsumedMessages = List[KeyedMessage[String, String]]()
    for (partitionId <- (0 to numPartitions - 1)) {
      val data = consumeMessagesFrom(config1, bootstrapBroker, topic, partitionId)
      allConsumedMessages = allConsumedMessages.concat(data.messages)
    }

    assert(allConsumedMessages.size == 3)
    assert(allConsumedMessages == messages)


    val partitionAndLeader: PartitionAndLeader = producer.leaderAndReplicasForKey("topic1", "AaAa")
    val partitionIdForMessage = partitionAndLeader.partitionId

    val info1: PartitionStateInfo = server1.apis.leaderCache(TopicAndPartition("topic1", partitionIdForMessage))
    val leaderBroker: Int = info1.leaderIsrAndControllerEpoch.leaderAndIsr.leader
    val replicas = info1.allReplicas

    val leaderServer = brokers(leaderBroker)
    val followerServers = replicas.filter(_ != leaderBroker).map(id => brokers(id)).toList

    def logOffsetFor(server: Server) = {
      server.logManager.getLog("topic1", partitionIdForMessage).get.logEndOffset
    }

    def matchLeaderAndFollowerOffsets = {
      val leaderOffset = logOffsetFor(leaderServer)
      val followerOffsets = followerServers.map(server => {
        val offset = logOffsetFor(server)
        offset
      })
      leaderOffset == 3 && followerOffsets(0) == leaderOffset && followerOffsets(1) == leaderOffset
    }

    TestUtils.waitUntilTrue(() => {
      matchLeaderAndFollowerOffsets
    }, s"Waiting till messages get replicated on all the servers", 4000)

    val leaderOffset = logOffsetFor(leaderServer)
    val followerOffsets = followerServers.map(server => logOffsetFor(server))
    assert(leaderOffset == 3)
    assert(followerOffsets(0) == leaderOffset && followerOffsets(1) == leaderOffset)
  }

  def leaderForAllPartitions(topic: String, numPartitions: Int, servers: List[Server]): Boolean = {
    val conditions = servers.map(server => {
      val topicPartitions = server.apis.leaderCache.keySet.filter(_.topic == topic)
      topicPartitions.size == numPartitions

    })
    conditions.filter(_ == false).size == 0
  }

  private def produceMessages(bootstrapBroker: InetAddressAndPort, config1: Config, message1: Seq[KeyedMessage[String, String]]) = {
    val producer = new Producer(bootstrapBroker, config1, new DefaultPartitioner[String]())
    message1.foreach(message => {
      producer.send(message)
    })
    producer
  }

  private def assertBrokersRegisteredWithZookeeper(config1: Config, config2: Config, config3: Config) = {

    val brokers: collection.Seq[ZkUtils.Broker] = ZkUtils.getAllBrokersInCluster(zkClient)

    assert(3 == brokers.size)

    val sortedBrokers = brokers.sortWith((b1, b2) => b1.id < b2.id)

    assert(sortedBrokers(0).id == config1.brokerId)
    assert(sortedBrokers(0).host == config1.hostName)
    assert(sortedBrokers(0).port == config1.port)

    assert(sortedBrokers(1).id == config2.brokerId)
    assert(sortedBrokers(1).host == config2.hostName)
    assert(sortedBrokers(1).port == config2.port)

    assert(sortedBrokers(2).id == config3.brokerId)
    assert(sortedBrokers(2).host == config3.hostName)
    assert(sortedBrokers(2).port == config3.port)
    zkClient
  }

  private def consumeMessagesFrom(config1: Config, bootstrapBroker: InetAddressAndPort, topic: String, partitionId: Int) = {
    val consumer = new Consumer("TestConsumer", bootstrapBroker, config1)
    val inetAddressAndPort = consumer.findCoordinator()
    consumer.read(topic, partitionId)
  }
}
