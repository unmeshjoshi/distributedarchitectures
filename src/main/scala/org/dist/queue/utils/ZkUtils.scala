package org.dist.queue.utils

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkMarshallingError, ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.zookeeper.data.Stat
import org.dist.kvstore.JsonSerDes
import org.dist.queue.{Logging, SystemTime}

import scala.collection.Seq

object ZkUtils extends Logging {
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
    import scala.collection.JavaConverters._
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
