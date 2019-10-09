package org.dist.queue.utils

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkMarshallingError, ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.zookeeper.data.Stat
import org.dist.kvstore.JsonSerDes
import org.dist.queue.common.{Logging, TopicAndPartition}
import org.dist.queue.controller.{Controller, LeaderAndIsr, LeaderIsrAndControllerEpoch}

import scala.collection.{Map, Seq, Set, mutable}

object ZkUtils extends Logging {

  def pathExists(client: ZkClient, path: String): Boolean = {
    client.exists(path)
  }


  def getLeaderIsrAndEpochForPartition(zkClient: ZkClient, topic: String, partition: Int):Option[LeaderIsrAndControllerEpoch] = {
    val leaderAndIsrPath = getTopicPartitionLeaderAndIsrPath(topic, partition)
    val leaderAndIsrInfo = readDataMaybeNull(zkClient, leaderAndIsrPath)
    val leaderAndIsrOpt = leaderAndIsrInfo._1
    val stat = leaderAndIsrInfo._2
    leaderAndIsrOpt match {
      case Some(leaderAndIsrStr) => parseLeaderAndIsr(leaderAndIsrStr, topic, partition, stat)
      case None => None
    }
  }

  def parseLeaderAndIsr(leaderAndIsrStr: String, topic: String, partition: Int, stat: Stat)
  : Option[LeaderIsrAndControllerEpoch] = {
    val leaderIsrAndEpochInfo = JsonSerDes.deserialize(leaderAndIsrStr.getBytes, classOf[Map[String, Any]])
        val leader = leaderIsrAndEpochInfo.get("leader").get.asInstanceOf[Int]
        val epoch = leaderIsrAndEpochInfo.get("leader_epoch").get.asInstanceOf[Int]
        val isr = leaderIsrAndEpochInfo.get("isr").get.asInstanceOf[List[Int]]
        val controllerEpoch = leaderIsrAndEpochInfo.get("controller_epoch").get.asInstanceOf[Int]
        val zkPathVersion = stat.getVersion
        debug("Leader %d, Epoch %d, Isr %s, Zk path version %d for partition [%s,%d]".format(leader, epoch,
          isr.toString(), zkPathVersion, topic, partition))
        Some(LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch))
  }

  def getTopicPartitionLeaderAndIsrPath(topic: String, partitionId: Int): String ={
    getTopicPartitionPath(topic, partitionId) + "/" + "state"
  }

  def getPartitionLeaderAndIsrForTopics(zkClient: ZkClient, topicAndPartitions: Set[TopicAndPartition])
  : mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = {
    val ret = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
    for(topicAndPartition <- topicAndPartitions) {
      ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topicAndPartition.topic, topicAndPartition.partition) match {
        case Some(leaderIsrAndControllerEpoch) => ret.put(topicAndPartition, leaderIsrAndControllerEpoch)
        case None =>
      }
    }
    ret
  }


  def getPartitionAssignmentForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[String, collection.Map[Int, Seq[Int]]] = {
    var ret = mutable.Map[String, Map[Int, Seq[Int]]]()
    topics.foreach{ topic =>
      val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1
      val partitionMap: Map[Int, Seq[Int]] = jsonPartitionMapOpt match {
        case Some(jsonPartitionMap) =>
          val partitionsOpt: Option[Any] = JsonSerDes.deserialize(jsonPartitionMap.getBytes(), classOf[Map[String, Any]]).get("partitions")
          partitionsOpt match {
            case Some(m) => m.asInstanceOf[Map[String, Any]].get("partitions") match {
              case Some(replicaMap) =>
                val m1: Map[String, Seq[Int]] = replicaMap.asInstanceOf[Map[String, Seq[Int]]]
                import java.util.HashMap

                import scala.jdk.CollectionConverters._

                val m2 = new HashMap[Int, Seq[Int]]()
                val keys = m1.keySet
                for(key <- keys) {
                  val value = m1(key)
                  m2.put(key.toInt, value)
                }
                m2.asScala
              case None => Map[Int, Seq[Int]]()
            }
            case None => Map[Int, Seq[Int]]()
          }
        case None => Map[Int, Seq[Int]]()
      }
      debug("Partition map for /brokers/topics/%s is %s".format(topic, partitionMap))
      ret.put(topic, partitionMap)
    }
    ret
  }


  def getReplicasForPartition(zkClient: ZkClient, topic: String, partition: Int): Seq[Int] = {
    val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1
    jsonPartitionMapOpt match {
      case Some(jsonPartitionMap) =>
        val partitionsOpt = JsonSerDes.deserialize(jsonPartitionMap.getBytes(), classOf[Map[String, Any]]).get("partitions")
        partitionsOpt match {
            case Some(replicaMap) => replicaMap.asInstanceOf[Map[String, Seq[Int]]].get(partition.toString) match {
              case Some(seq) => seq
              case None => Seq.empty[Int]
            }
            case None => Seq.empty[Int]
        }
      case None => Seq.empty[Int]
    }
  }

  def getReplicaAssignmentForTopics(zkClient: ZkClient, topics: scala.Seq[String])= {
    val ret = new mutable.HashMap[TopicAndPartition, Seq[Int]]
    topics.foreach { topic =>
      val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1
      jsonPartitionMapOpt match {
        case Some(jsonPartitionMap) => {
          val partitionsOpt = JsonSerDes.deserialize(jsonPartitionMap.getBytes(), classOf[Map[String, Any]]).get("partitions")
          partitionsOpt match {
            case Some(partitions) =>
              for((partition, replicas) <- partitions.asInstanceOf[Map[String, Seq[Int]]]){
                ret.put(TopicAndPartition(topic, partition.toInt), replicas)
                debug("Replicas assigned to topic [%s], partition [%s] are [%s]".format(topic, partition, replicas))
              }
            case None =>
          }
        }
        case None =>
      }
    }
    ret
  }


  def leaderAndIsrZkData(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int): String = {
    val isrInfo = Utils.seqToJson(leaderAndIsr.isr.map(_.toString), valueInQuotes = false)
    Utils.mapToJson(Map("version" -> 1.toString, "leader" -> leaderAndIsr.leader.toString, "leader_epoch" -> leaderAndIsr.leaderEpoch.toString,
      "controller_epoch" -> controllerEpoch.toString, "isr" -> isrInfo), valueInQuotes = false)
  }

  def getAllTopics(zkClient: ZkClient): Seq[String] = {
    val topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath)
    if(topics == null)
      Seq.empty[String]
    else
      topics
  }

  /**
   *  make sure a persistent path exists in ZK. Create the path if not exist.
   */
  def makeSurePersistentPathExists(client: ZkClient, path: String) {
    if (!client.exists(path))
      client.createPersistent(path, true) // won't throw NoNodeException or NodeExistsException
  }

  /**
   * Get JSON partition to replica map from zookeeper.
   */
  def replicaAssignmentZkdata(map: Map[String, Seq[Int]]): String = {
    val jsonReplicaAssignmentMap = Utils.mapWithSeqValuesToJson(map)
    Utils.mapToJson(Map("version" -> 1.toString, "partitions" -> jsonReplicaAssignmentMap), valueInQuotes = false)
  }
  /**
   * Update the value of a persistent node with the given path and data.
   * create parrent directory if necessary. Never throw NodeExistException.
   * Return the updated path zkVersion
   */
  def updatePersistentPath(client: ZkClient, path: String, data: String) = {
    try {
      client.writeData(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        try {
          client.createPersistent(path, data)
        } catch {
          case e: ZkNodeExistsException =>
            client.writeData(path, data)
          case e2: Throwable => throw e2
        }
      }
      case e2: Throwable => throw e2
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

  def createSequentialPersistentPath(client: ZkClient, path: String, data: String = ""): String = {
    client.createPersistentSequential(path, data)
  }


  def getSortedBrokerList(zkClient: ZkClient) = {
    ZkUtils.getChildren(zkClient, BrokerIdsPath).map(_.toInt).sorted
  }


  def getTopicPath(topic: String): String ={
    BrokerTopicsPath + "/" + topic
  }

  def getTopicPartitionsPath(topic: String): String ={
    getTopicPath(topic) + "/partitions"
  }

  def getController(zkClient: ZkClient): Int= {
    readDataMaybeNull(zkClient, ControllerPath)._1 match {
      case Some(controller) => Controller.parseControllerId(controller)
      case None => throw new RuntimeException("Controller doesn't exist")
    }
  }

  def getTopicPartitionPath(topic: String, partitionId: Int): String ={
    getTopicPartitionsPath(topic) + "/" + partitionId
  }


  def getChildren(client: ZkClient, path: String): Seq[String] = {
    import scala.jdk.CollectionConverters._
    // triggers implicit conversion from java list to scala Seq
    client.getChildren(path).asScala
  }


  val ConsumersPath = "/consumers"
  val BrokerIdsPath = "/brokers/ids"
  val BrokerTopicsPath = "/brokers/topics"
  val ControllerPath = "/controller"
  val ControllerEpochPath = "/controller_epoch"
  val ReassignPartitionsPath = "/admin/reassign_partitions"
  val PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election"


  def readDataMaybeNull(client: ZkClient, path: String): (Option[String], Stat) = {
    val stat: Stat = new Stat()
    val dataAndStat = try {
      (Some(client.readData(path, stat)), stat)
    } catch {
      case e: ZkNoNodeException =>
        (None, stat)
      case e2: Throwable => throw e2
    }
    dataAndStat
  }

  case class Broker(val id: Int, val host: String, val port: Int)

  object Broker {
    def createBroker(brokerId: Int, brokerInfo: String):Broker = {
      JsonSerDes.deserialize(brokerInfo.getBytes, classOf[Broker])
    }
  }

  def getAllBrokersInCluster(zkClient: ZkClient): Seq[Broker] = {
    val brokerIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath).sorted
    brokerIds.map(_.toInt).map(getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
  }

  def getBrokerInfo(zkClient: ZkClient, brokerId: Int): Option[Broker] = {
    ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
      case Some(brokerInfo) => Some(Broker.createBroker(brokerId, brokerInfo))
      case None => None
    }
  }

  def getChildrenParentMayNotExist(client: ZkClient, path: String): Seq[String] = {
    import scala.jdk.CollectionConverters._
    // triggers implicit conversion from java list to scala Seq
    try {
      client.getChildren(path).asScala
    } catch {
      case e: ZkNoNodeException => return Nil
      case e2: Throwable => throw e2
    }
  }

  def registerBrokerInZk(zkClient: ZkClient, id: Int, host: String, port: Int, timeout: Int, jmxPort: Int) {
    val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + id
    val timestamp = "\"" + SystemTime.milliseconds.toString + "\""
    val broker = new Broker(id, host, port)
    val brokerData = JsonSerDes.serialize(broker)
    try {
      createEphemeralPathExpectConflictHandleZKBug(zkClient, brokerIdPath, brokerData, broker,
        (brokerString: String, broker: Any) => JsonSerDes.deserialize(brokerString.getBytes, classOf[Broker]).equals(broker.asInstanceOf[Broker]),
        timeout)

    } catch {
      case e: ZkNodeExistsException =>
        throw new RuntimeException("A broker is already registered on the path " + brokerIdPath
          + ". This probably " + "indicates that you either have configured a brokerid that is already in use, or "
          + "else you have shutdown this broker and restarted it faster than the zookeeper "
          + "timeout so it appears to be re-registering.")
    }
    info("Registered broker %d at path %s with address %s:%d.".format(id, brokerIdPath, host, port))
  }

  /**
   * Create an ephemeral node with the given path and data.
   * Throw NodeExistsException if node already exists.
   * Handles the following ZK session timeout bug:
   *
   * https://issues.apache.org/jira/browse/ZOOKEEPER-1740
   *
   * Upon receiving a NodeExistsException, read the data from the conflicted path and
   * trigger the checker function comparing the read data and the expected data,
   * If the checker function returns true then the above bug might be encountered, back off and retry;
   * otherwise re-throw the exception
   */
  def createEphemeralPathExpectConflictHandleZKBug(zkClient: ZkClient, path: String, data: String, expectedCallerData: Any, checker: (String, Any) => Boolean, backoffTime: Int): Unit = {
    while (true) {
      try {
        createEphemeralPathExpectConflict(zkClient, path, data)
        return
      } catch {
        case e: ZkNodeExistsException => {
          // An ephemeral node may still exist even after its corresponding session has expired
          // due to a Zookeeper bug, in this case we need to retry writing until the previous node is deleted
          // and hence the write succeeds without ZkNodeExistsException
          ZkUtils.readDataMaybeNull(zkClient, path)._1 match {
            case Some(writtenData) => {
              if (checker(writtenData, expectedCallerData)) {
                info("I wrote this conflicted ephemeral node [%s] at %s a while back in a different session, ".format(data, path)
                  + "hence I will backoff for this node to be deleted by Zookeeper and retry")

                Thread.sleep(backoffTime)
              } else {
                throw e
              }
            }
            case None => // the node disappeared; retry creating the ephemeral node immediately
          }
        }
        case e2: Throwable => throw e2
      }
    }
  }

  def readData(client: ZkClient, path: String): (String, Stat) = {
    val stat: Stat = new Stat()
    val dataStr: String = client.readData(path, stat)
    (dataStr, stat)
  }
  /**
   * Create an ephemeral node with the given path and data.
   * Throw NodeExistException if node already exists.
   */
  def createEphemeralPathExpectConflict(client: ZkClient, path: String, data: String): Unit = {
    try {
      createEphemeralPath(client, path, data)
    } catch {
      case e: ZkNodeExistsException => {
        // this can happen when there is connection loss; make sure the data is what we intend to write
        var storedData: String = null
        try {
          storedData = readData(client, path)._1
        } catch {
          case e1: ZkNoNodeException => // the node disappeared; treat as if node existed and let caller handles this
          case e2: Throwable => throw e2
        }
        if (storedData == null || storedData != data) {
          info("conflict in " + path + " data: " + data + " stored data: " + storedData)
          throw e
        } else {
          // otherwise, the creation succeeded, return normally
          info(path + " exists with value " + data + " during connection loss; this is ok")
        }
      }
      case e2: Throwable => throw e2
    }
  }

  /**
   * Create an ephemeral node with the given path and data. Create parents if necessary.
   */
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

  /**
   *  create the parent path
   */
  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }
}


object ZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data : Object) : Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes : Array[Byte]) : Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}
