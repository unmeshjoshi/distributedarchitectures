package org.dist.simplekafka

import java.util.Random

import org.dist.queue.utils.AdminUtils.rand

import scala.collection.{Map, Seq, mutable}

case class PartitionReplicas(partitionId:Int, brokerIds:List[Int])

class CreateTopicCommand(zookeeperClient:ZookeeperClient, partitionAssigner:ReplicaAssignmentStrategy = new ReplicaAssignmentStrategy()) {
  val rand = new Random

  def createTopic(topicName:String, noOfPartitions:Int, replicationFactor:Int) = {
    val brokerIds = zookeeperClient.getAllBrokerIds()
    //get list of brokers
    //assign replicas to partition
    val partitionReplicas = assignReplicasToBrokers(brokerIds.toList, noOfPartitions, replicationFactor)
    // register topic with partition assignments to zookeeper
    zookeeperClient.setPartitionReplicasForTopic(topicName, partitionReplicas)

  }

  //on new topic creation
  //get partition assignments
  //select leader for each partition
  //send metadata request with leader and isr to all the brokers


  /**
   * There are 2 goals of replica assignment:
   * 1. Spread the replicas evenly among brokers.
   * 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.
   *
   * To achieve this goal, we:
   * 1. Assign the first replica of each partition by round-robin, starting from a random position in the broker list.
   * 2. Assign the remaining replicas of each partition with an increasing shift.
   *
   * Here is an example of assigning
   * broker-0  broker-1  broker-2  broker-3  broker-4
   * p0        p1        p2        p3        p4       (1st replica)
   * p5        p6        p7        p8        p9       (1st replica)
   * p4        p0        p1        p2        p3       (2nd replica)
   * p8        p9        p5        p6        p7       (2nd replica)
   * p3        p4        p0        p1        p2       (3nd replica)
   * p7        p8        p9        p5        p6       (3nd replica)
   */
  def assignReplicasToBrokers(brokerList: List[Int], nPartitions: Int, replicationFactor: Int) = {

    val ret = new mutable.HashMap[Int, List[Int]]()
    val startIndex = rand.nextInt(brokerList.size)
    var currentPartitionId = 0

    var nextReplicaShift = rand.nextInt(brokerList.size)
    for (partitionId <- 0 until nPartitions) {
      if (currentPartitionId > 0 && (currentPartitionId % brokerList.size == 0))
        nextReplicaShift += 1
      val firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size
      var replicaList = List(brokerList(firstReplicaIndex))
      for (j <- 0 until replicationFactor - 1)
        replicaList ::= brokerList(getWrappedIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size))
      ret.put(currentPartitionId, replicaList.reverse)
      currentPartitionId = currentPartitionId + 1
    }
    val partitionIds = ret.toMap.keySet
    partitionIds.map(id => PartitionReplicas(id, ret(id)))
  }


  private def getWrappedIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }
}

