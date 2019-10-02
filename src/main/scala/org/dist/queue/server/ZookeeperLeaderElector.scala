package org.dist.queue.server

import org.I0Itec.zkclient.IZkDataListener
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.dist.queue.common.Logging
import org.dist.queue.controller.{Controller, ControllerContext}
import org.dist.queue.utils.ZkUtils._
import org.dist.queue.utils.{SystemTime, Utils, ZkUtils}


class ZookeeperLeaderElector(controllerContext: ControllerContext, electionPath: String, onBecomingLeader: () => Unit,
                             brokerId: Int) {
  // create the election path in ZK, if one does not exist
  val index = electionPath.lastIndexOf("/")
  val leaderChangeListener = new LeaderChangeListener
  if (index > 0)
    ZkUtils.makeSurePersistentPathExists(controllerContext.zkClient, electionPath.substring(0, index))
  var leaderId = -1

  def startup() = {
    controllerContext.zkClient.subscribeDataChanges(electionPath, leaderChangeListener)
    elect
  }

  def elect: Boolean = {
    val timestamp = SystemTime.milliseconds.toString
    val electString =
      Utils.mergeJsonFields(Utils.mapToJsonFields(Map("version" -> 1.toString, "brokerid" -> brokerId.toString), valueInQuotes = false)
        ++ Utils.mapToJsonFields(Map("timestamp" -> timestamp), valueInQuotes = true))
    try {
      createEphemeralPathExpectConflictHandleZKBug(controllerContext.zkClient, electionPath, electString, brokerId,
        (controllerString: String, leaderId: Any) => Controller.parseControllerId(controllerString) == leaderId.asInstanceOf[Int],
        controllerContext.zkSessionTimeoutMs)
      info(brokerId + " successfully elected as leader")
      leaderId = brokerId
      onBecomingLeader()
    } catch {
      case e: ZkNodeExistsException =>
        // If someone else has written the path, then
        leaderId = readDataMaybeNull(controllerContext.zkClient, electionPath)._1 match {
          case Some(controller) => Controller.parseControllerId(controller)
          case None => {
            warn("A leader has been elected but just resigned, this will result in another round of election")
            -1
          }
        }
        if (leaderId != -1)
          debug("Broker %d was elected as leader instead of broker %d".format(leaderId, brokerId))
      case e2: Throwable =>
        error("Error while electing or becoming leader on broker %d".format(brokerId), e2)
        leaderId = -1
    }

    amILeader
  }

  def amILeader: Boolean = leaderId == brokerId

  /**
   * We do not have session expiration listen in the ZkElection, but assuming the caller who uses this module will
   * have its own session expiration listener and handler
   */
  class LeaderChangeListener extends IZkDataListener with Logging {
    /**
     * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
     *
     * @throws Exception On any error.
     */
    @throws(classOf[Exception])
    def handleDataChange(dataPath: String, data: Object) {
      leaderId = Controller.parseControllerId(data.toString)
      info("New leader is %d".format(leaderId))
    }

    /**
     * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
     *
     * @throws Exception
     * On any error.
     */
    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
      debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader"
        .format(brokerId, dataPath))
      elect
    }
  }

}


