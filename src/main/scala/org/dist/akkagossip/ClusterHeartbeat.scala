package org.dist.akkagossip

import org.dist.patterns.common.InetAddressAndPort
import org.dist.queue.common.Logging

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import scala.annotation.tailrec
import scala.collection.immutable

trait FailureDetector {

  def isMonitoring(): Boolean

  def heartbeat()

  def isAvailable():Boolean

  def remove()
}



/**
 * Sent at regular intervals for failure detection.
 */
final case class Heartbeat(override val from: InetAddressAndPort) extends Message(from)

/**
 * Sent as reply to [[Heartbeat]] messages.
 */
final case class HeartbeatRsp(override val from: InetAddressAndPort) extends Message(from)


class ClusterHeartbeat(clusterDaemon: ClusterDaemon,
                       selfUniqueAddress:InetAddressAndPort,
                       MonitoredByNrOfMembers:Int,
                       failureDetectorRegistry: FailureDetectorRegistry[InetAddressAndPort]) extends Logging {
  def heartbeatResponse(heartbeatRsp: HeartbeatRsp) = {
    info(s"received heartbeat response from ${heartbeatRsp.from}")
    state = state.heartbeatRsp(heartbeatRsp.from)
  }


  def addMember(m: Member):Unit = {
    state = state.addMember(m.uniqueAddress)
  }

  var state = ClusterHeartbeatSenderState(
    ring = HeartbeatNodeRing(selfUniqueAddress, Set(selfUniqueAddress), Set.empty, MonitoredByNrOfMembers),
    oldReceiversNowUnreachable = Set.empty[InetAddressAndPort],
    failureDetectorRegistry)
  val selfHeartbeat = Heartbeat(selfUniqueAddress)

  private val heartbeatScheduler = new ScheduledThreadPoolExecutor(1)



  def start() = {
    heartbeatScheduler.scheduleAtFixedRate(() => {
      heartbeat()
    }, 1000,
      1000, TimeUnit.MILLISECONDS)

  }

  def heartbeat(): Unit = {
    state.activeReceivers foreach { to ⇒
      if (failureDetectorRegistry.isMonitoring(to)) {
        info(s"Cluster Node [${selfUniqueAddress}] - Heartbeat to [${to.address}]")
      } else {
        info(s"Cluster Node [${selfUniqueAddress}] - First Heartbeat to [${to.address}]")
//         schedule the expected first heartbeat for later, which will give the
                // other side a chance to reply, and also trigger some resends if needed
//                scheduler.scheduleOnce(HeartbeatExpectedResponseAfter, self, ExpectedFirstHeartbeat(to))
      }
      clusterDaemon.networkIO.send(to, selfHeartbeat)
    }
  }
}

case class ClusterHeartbeatSenderState(ring:HeartbeatNodeRing,
                                       oldReceiversNowUnreachable:Set[InetAddressAndPort],
                                       failureDetector:FailureDetectorRegistry[InetAddressAndPort]) {

  val activeReceivers: Set[InetAddressAndPort] = ring.myReceivers union oldReceiversNowUnreachable

  def selfAddress = ring.selfAddress

  def init(nodes: Set[InetAddressAndPort], unreachable: Set[InetAddressAndPort]): ClusterHeartbeatSenderState =
    copy(ring = ring.copy(nodes = nodes + selfAddress, unreachable = unreachable))

  def contains(node: InetAddressAndPort): Boolean = ring.nodes(node)

  def addMember(node: InetAddressAndPort): ClusterHeartbeatSenderState =
    membershipChange(ring :+ node)

  def removeMember(node: InetAddressAndPort): ClusterHeartbeatSenderState = {
    val newState = membershipChange(ring :- node)

    failureDetector remove node
    if (newState.oldReceiversNowUnreachable(node))
      newState.copy(oldReceiversNowUnreachable = newState.oldReceiversNowUnreachable - node)
    else
      newState
  }

  def unreachableMember(node: InetAddressAndPort): ClusterHeartbeatSenderState =
    membershipChange(ring.copy(unreachable = ring.unreachable + node))

  def reachableMember(node: InetAddressAndPort): ClusterHeartbeatSenderState =
    membershipChange(ring.copy(unreachable = ring.unreachable - node))

  private def membershipChange(newRing: HeartbeatNodeRing): ClusterHeartbeatSenderState = {
    val oldReceivers = ring.myReceivers
    val removedReceivers = oldReceivers diff newRing.myReceivers
    var adjustedOldReceiversNowUnreachable = oldReceiversNowUnreachable
    removedReceivers foreach { a ⇒
      if (failureDetector.isAvailable(a))
        failureDetector remove a
      else
        adjustedOldReceiversNowUnreachable += a
    }
    copy(newRing, adjustedOldReceiversNowUnreachable)
  }

  def heartbeatRsp(from: InetAddressAndPort): ClusterHeartbeatSenderState =
    if (activeReceivers(from)) {
      failureDetector heartbeat from
      if (oldReceiversNowUnreachable(from)) {
        // back from unreachable, ok to stop heartbeating to it
        if (!ring.myReceivers(from))
          failureDetector remove from
        copy(oldReceiversNowUnreachable = oldReceiversNowUnreachable - from)
      } else this
    } else this

}

/**
 * INTERNAL API
 *
 * Data structure for picking heartbeat receivers. The node ring is
 * shuffled by deterministic hashing to avoid picking physically co-located
 * neighbors.
 *
 * It is immutable, i.e. the methods return new instances.
 */
case class HeartbeatNodeRing(selfAddress:            InetAddressAndPort,
                             nodes:                  Set[InetAddressAndPort],
                             unreachable:            Set[InetAddressAndPort],
                             monitoredByNrOfMembers: Int) {

  require(nodes contains selfAddress, s"nodes [${nodes.mkString(", ")}] must contain selfAddress [${selfAddress}]")

  private val nodeRing: immutable.SortedSet[InetAddressAndPort] = {
    implicit val ringOrdering: Ordering[InetAddressAndPort] = Ordering.fromLessThan[InetAddressAndPort] { (a, b) ⇒
      val ha = a.##
      val hb = b.##
      ha < hb || (ha == hb && Member.addressOrdering.compare(a, b) < 0)
    }

    immutable.SortedSet() union nodes
  }

  /**
   * Receivers for `selfAddress`. Cached for subsequent access.
   */
  lazy val myReceivers: immutable.Set[InetAddressAndPort] = receivers(selfAddress)

  private val useAllAsReceivers = monitoredByNrOfMembers >= (nodeRing.size - 1)

  /**
   * The receivers to use from a specified sender.
   */
  def receivers(sender: InetAddressAndPort): Set[InetAddressAndPort] =
    if (useAllAsReceivers)
      nodeRing - sender
    else {

      // Pick nodes from the iterator until n nodes that are not unreachable have been selected.
      // Intermediate unreachable nodes up to `monitoredByNrOfMembers` are also included in the result.
      // The reason for not limiting it to strictly monitoredByNrOfMembers is that the leader must
      // be able to continue its duties (e.g. removal of downed nodes) when many nodes are shutdown
      // at the same time and nobody in the remaining cluster is monitoring some of the shutdown nodes.
      // This was reported in issue #16624.
      @tailrec def take(n: Int, iter: Iterator[InetAddressAndPort], acc: Set[InetAddressAndPort]): (Int, Set[InetAddressAndPort]) =
        if (iter.isEmpty || n == 0) (n, acc)
        else {
          val next = iter.next()
          val isUnreachable = unreachable(next)
          if (isUnreachable && acc.size >= monitoredByNrOfMembers)
            take(n, iter, acc) // skip the unreachable, since we have already picked `monitoredByNrOfMembers`
          else if (isUnreachable)
            take(n, iter, acc + next) // include the unreachable, but don't count it
          else
            take(n - 1, iter, acc + next) // include the reachable
        }

      val (remaining, slice1) = take(monitoredByNrOfMembers, nodeRing.from(sender).tail.iterator, Set.empty)
      val slice =
        if (remaining == 0)
          slice1
        else {
          // wrap around
          val (_, slice2) = take(remaining, nodeRing.to(sender).iterator.filterNot(_ == sender), slice1)
          slice2
        }

      slice
    }

  /**
   * Add a node to the ring.
   */
  def :+(node: InetAddressAndPort): HeartbeatNodeRing = if (nodes contains node) this else copy(nodes = nodes + node)

  /**
   * Remove a node from the ring.
   */
  def :-(node: InetAddressAndPort): HeartbeatNodeRing =
    if (nodes.contains(node) || unreachable.contains(node))
      copy(nodes = nodes - node, unreachable = unreachable - node)
    else this

}
