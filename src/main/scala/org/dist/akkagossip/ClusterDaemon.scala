package org.dist.akkagossip

import org.dist.akkagossip.MemberStatus._
import org.dist.patterns.common.InetAddressAndPort
import org.dist.queue.common.Logging

import java.util
import java.util.Collections
import java.util.concurrent.{ScheduledThreadPoolExecutor, ThreadLocalRandom}


class ClusterDaemon(selfUniqueAddress:InetAddressAndPort) extends Logging {
  def join(seedNode: InetAddressAndPort) = {
    networkIO.send(seedNode, Join(selfUniqueAddress))
  }

  val vclockNode = VectorClock.Node(Gossip.vclockName(selfUniqueAddress))

  /**
   * The types of gossip actions that receive gossip has performed.
   */
  sealed trait ReceiveGossipType
  case object Ignored extends ReceiveGossipType
  case object Older extends ReceiveGossipType
  case object Newer extends ReceiveGossipType
  case object Same extends ReceiveGossipType
  case object Merge extends ReceiveGossipType

  //</codeFragment>
  private val leaderActionExecutor = new ScheduledThreadPoolExecutor(1)
  leaderActionExecutor.scheduleAtFixedRate(()=>{
    ClusterDaemon.this.leaderActions()
  }, 1000, 1000, java.util.concurrent.TimeUnit.MILLISECONDS)

  private val gossipExecutor = new ScheduledThreadPoolExecutor(1)
  gossipExecutor.scheduleAtFixedRate(()=>{
    gossipRandomN(2)
  }, 1000, 1000, java.util.concurrent.TimeUnit.MILLISECONDS)

  def isSingletonCluster: Boolean = latestGossip.isSingletonCluster

  def validNodeForGossip(node: InetAddressAndPort): Boolean =
    (node != selfUniqueAddress && latestGossip.hasMember(node))

  def gossipRandomN(n: Int): Unit = {
    if (!isSingletonCluster && n > 0) {
      val localGossip = latestGossip
      // using ArrayList to be able to shuffle
      val possibleTargets = new util.ArrayList[InetAddressAndPort](localGossip.members.size)
      localGossip.members.foreach { m ⇒
        if (validNodeForGossip(m.uniqueAddress))
          possibleTargets.add(m.uniqueAddress)
      }
      val randomTargets =
        if (possibleTargets.size <= n)
          possibleTargets
        else {
          Collections.shuffle(possibleTargets, ThreadLocalRandom.current())
          possibleTargets.subList(0, n)
        }

      val iter = randomTargets.iterator
      while (iter.hasNext)
        gossipTo(iter.next())
    }
  }

  def receive(message: Message) = {
    message match {
      case envelope: GossipEnvelope =>
        handleGossip(envelope)
      case welcome:Welcome =>
        handleWelcome(welcome)
      case join:Join =>
        joining(join.address)
    }
  }


  def handleGossip(envelope: GossipEnvelope) = {
    val remoteGossip = envelope.latestGossip
    val localGossip = latestGossip
    println(s"Received Gossip from ${envelope.from}")
    val comparison = remoteGossip.version.compareTo(localGossip.version)

    val (winningGossip, talkback, gossipType) = comparison match {
      case VectorClock.Same =>
        // same version
        val talkback = !remoteGossip.seenByNode(selfUniqueAddress)
        (remoteGossip.mergeSeen(localGossip), talkback, Same)
      case VectorClock.Before =>
        // local is newer
        (localGossip, true, Older)
      case VectorClock.After =>
        // remote is newer
        val talkback = !remoteGossip.seenByNode(selfUniqueAddress)
        (remoteGossip, talkback, Newer)
    }

    membershipState = membershipState.copy(
      latestGossip = winningGossip.seen(selfUniqueAddress))

    if (talkback)
      gossipTo(envelope.from)

    println(membershipState)
  }

  def publishMembershipState() = {}

  def becomeInitialized(): Unit = {}

  def handleWelcome(welcome: Welcome): Unit = {
    require(latestGossip.members.isEmpty, "Join can only be done from empty state")
    membershipState = membershipState.copy(latestGossip = welcome.latestGossip).seen()
    info(s"Welcome from [${welcome.from}]")
    assertLatestGossip()
    publishMembershipState()
    if (welcome.from != selfUniqueAddress)
        gossipTo(welcome.from)
    becomeInitialized()
  }


  var networkIO = new DirectNetworkIO();

  var membershipState = MembershipState(
    Gossip.empty, selfUniqueAddress)

  var isCurrentlyLeader = false

  def latestGossip: Gossip = membershipState.latestGossip

  var exitingTasksInProgress = false

  def assertLatestGossip(): Unit =
    if (latestGossip.version.versions.size > latestGossip.members.size)
      throw new IllegalStateException(s"Too many vector clock entries in gossip state $latestGossip")


  def updateLatestGossip(gossip: Gossip): Unit = {
    // Updating the vclock version for the changes
    val versionedGossip = gossip :+ vclockNode

    // Don't mark gossip state as seen while exiting is in progress, e.g.
    // shutting down singleton actors. This delays removal of the member until
    // the exiting tasks have been completed.
    val newGossip =
    if (exitingTasksInProgress)
      versionedGossip.clearSeen()
    else {
      // Nobody else has seen this gossip but us
      val seenVersionedGossip = versionedGossip.onlySeen(selfUniqueAddress)
      // Update the state with the new gossip
      seenVersionedGossip
    }
    membershipState = membershipState.copy(newGossip)
    assertLatestGossip()
  }

  /**
   * State transition to DOWN.
   * Its status is set to DOWN. The node is also removed from the `seen` table.
   *
   * The node will eventually be removed by the leader, and only after removal a new node with same address can
   * join the cluster through the normal joining procedure.
   */
  def downing(address: InetAddressAndPort): Unit = {
    val localGossip = latestGossip
    val localMembers = localGossip.members
//    val localReachability = membershipState.dcReachability

    // check if the node to DOWN is in the `members` set
    localMembers.find(_.address == address) match {
      case Some(m) if m.status != Down =>
//        if (localReachability.isReachable(m.uniqueAddress))
//          logInfo(
//            ClusterLogMarker.memberChanged(m.uniqueAddress, MemberStatus.Down),
//            "Marking node [{}] as [{}]",
//            m.address,
//            Down)
//        else
//          logInfo(
//            ClusterLogMarker.memberChanged(m.uniqueAddress, MemberStatus.Down),
//            "Marking unreachable node [{}] as [{}]",
//            m.address,
//            Down)
//
//        val newGossip = localGossip.markAsDown(m)
//        updateLatestGossip(newGossip)
//        publishMembershipState()
//        if (address == cluster.selfAddress) {
//          // spread the word quickly, without waiting for next gossip tick
//          gossipRandomN(MaxGossipsBeforeShuttingDownMyself)
//        } else {
//          // try to gossip immediately to downed node, as a STONITH signal
//          gossipTo(m.uniqueAddress)
//        }
//      case Some(_) => // already down
//      case None =>
//        logInfo("Ignoring down of unknown node [{}]", address)
    }

  }
  var leaderActionCounter = 0
  var exitingConfirmed = Set.empty[InetAddressAndPort]

  def publish(latestGossip: Gossip) = {
  }

  def leaderActionsOnConvergence() = {
    info("Leader actions on convergence")
    val localGossip = latestGossip
    val localMembers = localGossip.members
    val localOverview = localGossip.overview
    val localSeen = localOverview.seen

    val changedMembers = {
      val enoughMembers: Boolean = isMinNrOfMembersFulfilled
      def isJoiningToUp(m: Member): Boolean = (m.status == Joining || m.status == WeaklyUp) && enoughMembers

      latestGossip.members.collect {
        var upNumber = 0

        {
          case m if isJoiningToUp(m) =>
            // Move JOINING => UP (once all nodes have seen that this node is JOINING, i.e. we have a convergence)
            // and minimum number of nodes have joined the cluster
            if (upNumber == 0) {
              // It is alright to use same upNumber as already used by a removed member, since the upNumber
              // is only used for comparing age of current cluster members (Member.isOlderThan)
              val youngest = membershipState.youngestMember
              upNumber = 1 + (if (youngest.upNumber == Int.MaxValue) 0 else youngest.upNumber)
            } else {
              upNumber += 1
            }
            m.copyUp(upNumber)

          case m if m.status == Leaving =>
            // Move LEAVING => EXITING (once we have a convergence on LEAVING)
            m.copy(status = Exiting)
        }
      }
    }

    if (changedMembers.nonEmpty) {
      // handle changes

      // replace changed members
      val newMembers = changedMembers

      // removing REMOVED nodes from the `seen` table
      val removed = Set[InetAddressAndPort]()
      val newSeen = localSeen diff removed
      // removing REMOVED nodes from the `reachability` table
      val newReachability = localOverview.reachability.remove(removed)
      val newOverview = localOverview copy (seen = newSeen, reachability = newReachability)
      // Clear the VectorClock when member is removed. The change made by the leader is stamped
      // and will propagate as is if there are no other changes on other nodes.
      // If other concurrent changes on other nodes (e.g. join) the pruning is also
      // taken care of when receiving gossips.
      val newVersion = removed.foldLeft(localGossip.version) { (v, node) ⇒
        v.prune(VectorClock.Node(Gossip.vclockName(node)))
      }
      val newGossip = localGossip copy (members = newMembers, overview = newOverview, version = newVersion)

      updateLatestGossip(newGossip)

      // log status changes
      changedMembers foreach { m ⇒
        info(s"Leader is moving node [${m.address}] to [${m.status}]")
      }
      publish(latestGossip)
    }
  }

  val MinNrOfMembers = 2
  def isMinNrOfMembersFulfilled: Boolean = {
    latestGossip.members.size >= MinNrOfMembers
  }
  def leaderActions() = {
    info("doing leader actions")
    if (membershipState.isLeader(selfUniqueAddress)) {
      info(s"${selfUniqueAddress} is leader" )
      if (!isCurrentlyLeader) {
        info(s"${selfUniqueAddress} is the new leader among reachable nodes (more leaders may exist)")
        isCurrentlyLeader = true
      }
      val firstNotice = 20
      val periodicNotice = 60
      if (membershipState.convergence(exitingConfirmed)) {
        if (leaderActionCounter >= firstNotice)
          info("Leader can perform its duties again")
        leaderActionCounter = 0
        leaderActionsOnConvergence()
      } else {

      }
    } else {
      info("no leader elected yet " + membershipState)
    }
  }

  /**
   * State transition to JOINING - new node joining.
   * Received `Join` message and replies with `Welcome` message, containing
   * current gossip state, including the new joining member.
   */
  def joining(joiningNode: InetAddressAndPort): Unit = {
    val selfStatus = latestGossip.member(selfUniqueAddress).status
    val localMembers = latestGossip.members
    localMembers.find(_.address == joiningNode.getAddress) match {
      case Some(m) if m.uniqueAddress == joiningNode =>
        // node retried join attempt, probably due to lost Welcome message
        info(s"Existing member [{m}] is joining again.")
        if (joiningNode != selfUniqueAddress)
          networkIO.send(joiningNode, Welcome(selfUniqueAddress, joiningNode, latestGossip))
      //          sender() ! Welcome(selfUniqueAddress, latestGossip)
      case None =>
        // remove the node from the failure detector
        //        failureDetector.remove(joiningNode.address)
        //        crossDcFailureDetector.remove(joiningNode.address)

        // add joining node as Joining
        // add self in case someone else joins before self has joined (Set discards duplicates)
        val newMembers = localMembers + Member(joiningNode) + Member(selfUniqueAddress)
        val newGossip = latestGossip.copy(members = newMembers)

        updateLatestGossip(newGossip)

        if (joiningNode == selfUniqueAddress) {
          info(
            s"Node [${joiningNode.getAddress}] is JOINING itself forming new cluster")
          if (localMembers.isEmpty){}
//            leaderActions() //TODO: Figure out why this is important for deterministic oldest when bootstrapping
        } else {
            info(
              s"Node [${joiningNode.getAddress}] is JOINING")
            networkIO.send(joiningNode, Welcome(selfUniqueAddress, joiningNode, latestGossip))
          }
        }
  }

  def gossipTo(node: InetAddressAndPort): Unit =
    if (membershipState.validNodeForGossip(node))
       networkIO.send(node, GossipEnvelope(selfUniqueAddress, node, latestGossip))
}
