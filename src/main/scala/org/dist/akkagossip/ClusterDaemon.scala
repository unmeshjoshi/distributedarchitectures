package org.dist.akkagossip

import org.dist.akkagossip.ClusterEvent._
import org.dist.akkagossip.MemberStatus._
import org.dist.patterns.common.InetAddressAndPort
import org.dist.patterns.singularupdatequeue.SingularUpdateQueue
import org.dist.queue.common.Logging

import java.util
import java.util.Collections
import java.util.concurrent.{CompletableFuture, ScheduledThreadPoolExecutor, ThreadLocalRandom, TimeUnit}
import scala.collection.immutable
import scala.collection.immutable.VectorBuilder
import scala.concurrent.duration.Duration

class ClusterDaemon(val selfUniqueAddress: InetAddressAndPort) extends Logging {
  def oldestMember() = {
    immutable.SortedSet.empty(ageOrdering).union(membershipState.members).firstKey
  }

  def allMembersUp(clusterSize: Int) = {
    membershipState.allMembersUp(clusterSize)
  }

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
  private val scheduler = new ScheduledThreadPoolExecutor(1)
  scheduler.scheduleAtFixedRate(() => {
    q.submit(LeaderActionTick)

  }, 1000, 1000, java.util.concurrent.TimeUnit.MILLISECONDS)

  scheduler.scheduleAtFixedRate(() => {
    q.submit(GossipTick)
  }, 1000, 1000, java.util.concurrent.TimeUnit.MILLISECONDS)


  scheduler.scheduleAtFixedRate(() => {
    q.submit(ReapUnreachableTick)
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
    val value: CompletableFuture[Message] = q.submit(message)
  }

  case object  LeaderActionTick extends Message(selfUniqueAddress)
  case object GossipTick extends Message(selfUniqueAddress)
  case object ReapUnreachableTick extends Message(selfUniqueAddress)

  def sendHeartbeatResponse(from: InetAddressAndPort):Unit = {
    info(s"Sending heartbeat response from ${selfUniqueAddress} to ${from}")
    networkIO.send(from, HeartbeatRsp(selfUniqueAddress))
  }
  private def handleReceive(message: Message) = {
    message match {
      case GossipTick =>
        gossipRandomN(2)
      case LeaderActionTick =>
        leaderActions()
      case envelope: GossipEnvelope =>
        handleGossip(envelope)
      case welcome: Welcome =>
        handleWelcome(welcome)
      case join: Join =>
        joining(join.fromAddress)
      case heartbeat: Heartbeat =>
        sendHeartbeatResponse(heartbeat.from)
      case heartbeatRsp: HeartbeatRsp =>
          heartbeatResponse(heartbeatRsp)
      case  ReapUnreachableTick =>
        reapUnreachableMembers()

    }
  }

  private def heartbeatResponse(heartbeatRsp: HeartbeatRsp) = {
    heartbeat.heartbeatResponse(heartbeatRsp)
  }

  def reapUnreachableMembers(): Unit = {
    if (!isSingletonCluster) {
      // only scrutinize if we are a non-singleton cluster

      val localGossip = latestGossip
      val localOverview = localGossip.overview
      val localMembers = localGossip.members

      val newlyDetectedUnreachableMembers = localMembers filterNot { member ⇒
        member.uniqueAddress == selfUniqueAddress ||
          localOverview.reachability.status(selfUniqueAddress, member.uniqueAddress) == Reachability.Unreachable ||
          localOverview.reachability.status(selfUniqueAddress, member.uniqueAddress) == Reachability.Terminated ||
          failureDetector.isAvailable(member.address)
      }

      if (newlyDetectedUnreachableMembers.nonEmpty) {
        info(s"newly detected UNREACHABLE members in ${selfUniqueAddress} is ${newlyDetectedUnreachableMembers}")
      }
      val newlyDetectedReachableMembers = localOverview.reachability.allUnreachableFrom(selfUniqueAddress) collect {
        case node if node != selfUniqueAddress && failureDetector.isAvailable(node) ⇒
          localGossip.member(node)
      }

      if (newlyDetectedUnreachableMembers.nonEmpty || newlyDetectedReachableMembers.nonEmpty) {

        val reachability1: Reachability = localOverview.reachability
        val newReachability1 = (reachability1 /: newlyDetectedUnreachableMembers) {
          (reachability, m) ⇒ reachability.unreachable(selfUniqueAddress, m.uniqueAddress)
        }
        val newReachability2 = (newReachability1 /: newlyDetectedReachableMembers) {
          (reachability, m) ⇒ reachability.reachable(selfUniqueAddress, m.uniqueAddress)
        }

        if (newReachability2 ne localOverview.reachability) {
          val newOverview = localOverview copy (reachability = newReachability2)
          val newGossip = localGossip copy (overview = newOverview)

          val oldMembershipState = updateLatestGossip(newGossip)

          val (exiting, nonExiting) = newlyDetectedUnreachableMembers.partition(_.status == Exiting)
          if (nonExiting.nonEmpty)
            info(s"Cluster Node [${selfUniqueAddress}] - Marking node(s) as UNREACHABLE [${nonExiting.mkString(", ")}]. Node roles [{}]")
          if (exiting.nonEmpty)
            info(
              s"Marking exiting node(s) as UNREACHABLE [${exiting.mkString(", ")}]. This is expected and they will be removed.")
          if (newlyDetectedReachableMembers.nonEmpty)
            info(s"Marking node(s) as REACHABLE [${newlyDetectedReachableMembers.mkString(", ")}]. Node roles [{}]" )

          publishChanges(oldMembershipState, membershipState)
        }
      }
    }
  }

  def isUnreachableInFailureDetector(address:InetAddressAndPort) = !isAliveInFailureDetector(address)

  def isAliveInFailureDetector(address: InetAddressAndPort) = {
    heartbeat.state.failureDetector.isAvailable(address)
  }

  def publishChanges(oldMembershipState: MembershipState, membershipState: MembershipState) = {
    diffMemberEvents(oldMembershipState.latestGossip, membershipState.latestGossip) foreach publish
  }


  import Member._

  /**
   * INTERNAL API.
   */
  private def diffMemberEvents(oldGossip: Gossip, newGossip: Gossip): immutable.Seq[MemberEvent] =
    if (newGossip eq oldGossip) Nil
    else {
      val newMembers = newGossip.members diff oldGossip.members
      val membersGroupedByAddress: Map[InetAddressAndPort, List[Member]] = List(newGossip.members, oldGossip.members).flatten.groupBy(_.uniqueAddress)
      val changedMembers = membersGroupedByAddress collect {
        case (_, newMember :: oldMember :: Nil) if newMember.status != oldMember.status || newMember.upNumber != oldMember.upNumber ⇒
          newMember
      }
      val memberEvents = (newMembers ++ changedMembers).unsorted collect {
        case m if m.status == Joining ⇒ MemberJoined(m)
        case m if m.status == WeaklyUp ⇒ MemberWeaklyUp(m)
        case m if m.status == Up ⇒ MemberUp(m)
        case m if m.status == Leaving ⇒ MemberLeft(m)
        case m if m.status == Exiting ⇒ MemberExited(m)
      }

      val removedMembers = oldGossip.members diff newGossip.members
      val removedEvents = removedMembers.unsorted.map(m ⇒ MemberRemoved(m.copy(status = Removed), m.status))

      (new VectorBuilder[MemberEvent]() ++= memberEvents ++= removedEvents).result()
    }


  def handleGossip(envelope: GossipEnvelope) = {
    val remoteGossip = envelope.latestGossip
    val localGossip = latestGossip


    val comparison = remoteGossip.version.compareTo(localGossip.version)

    info(s"Received Gossip from ${envelope.from} in ${selfUniqueAddress} comparison=${comparison}")

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
      case _ =>
        // conflicting versions, merge
        val mergedGossip = remoteGossip merge localGossip
        val talkback = !remoteGossip.seenByNode(selfUniqueAddress)
        (mergedGossip, talkback, Merge)

    }

    var oldMembershipState = membershipState
    membershipState = membershipState.copy(
      latestGossip = winningGossip.seen(selfUniqueAddress))

    publishChanges(oldMembershipState, membershipState)


    if (talkback)
      gossipTo(envelope.from)

    info(s"${membershipState}")
  }

  def publishMembershipState() = {}

  def becomeInitialized(): Unit = {}

  def handleWelcome(welcome: Welcome): Unit = {
    require(latestGossip.members.isEmpty, "Join can only be done from empty state")
    val oldMembershipState = membershipState
    membershipState = membershipState.copy(latestGossip = welcome.latestGossip).seen()
    info(s"Welcome from [${welcome.from}]")
    assertLatestGossip()
    publishMembershipState()
    publishChanges(oldMembershipState, membershipState)
    if (welcome.from != selfUniqueAddress)
      gossipTo(welcome.from)
    becomeInitialized()
  }


  var networkIO = new DirectNetworkIO(Map());


  var membershipState = MembershipState(
    Gossip.empty, selfUniqueAddress)

  var isCurrentlyLeader = false

  def latestGossip: Gossip = membershipState.latestGossip

  var exitingTasksInProgress = false

  def assertLatestGossip(): Unit =
    if (latestGossip.version.versions.size > latestGossip.members.size)
      throw new IllegalStateException(s"Too many vector clock entries in gossip state $latestGossip")


  def updateLatestGossip(gossip: Gossip) = {
    // Updating the vclock version for the changes
    val versionedGossip = gossip :+ vclockNode

    // Don't mark gossip state as seen while exiting is in progress, e.g.
    // shutting down singleton actors. This delays removal of the member until
    // the exiting tasks have been completed.
    val newGossip = {
      if (exitingTasksInProgress)
        versionedGossip.clearSeen()
      else {
        // Nobody else has seen this gossip but us
        val seenVersionedGossip = versionedGossip.onlySeen(selfUniqueAddress)
        // Update the state with the new gossip
        seenVersionedGossip
      }
    }
    val oldMembershipState = membershipState
    membershipState = membershipState.copy(newGossip)
    assertLatestGossip()
    oldMembershipState
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

  val failureDetector = new DefaultFailureDetectorRegistry[InetAddressAndPort](()=> new DeadlineFailureDetector(Duration(3, TimeUnit.SECONDS), Duration(10, TimeUnit.SECONDS)))
  val MonitoredByNrOfMembers = 5
  val heartbeat = new ClusterHeartbeat(this, selfUniqueAddress, MonitoredByNrOfMembers, failureDetector);

  def startHeartbeating() = {
    heartbeat.start()
    println(s"Node ${selfUniqueAddress} joined the cluster. Starting heartbeats")
  }

  val ageOrdering = Member.ageOrdering
  var membersByAge: immutable.SortedSet[Member] = immutable.SortedSet.empty(ageOrdering)

  var changes = Vector.empty[OldestChanged]
  /**
   * The first event, corresponding to CurrentClusterState.
   */
  final case class InitialOldestState(oldest: Option[InetAddressAndPort], safeToBeOldest: Boolean)

  final case class OldestChanged(oldest: Option[InetAddressAndPort])


  def gotoOldest() = {

  }
  sealed trait State
  case object Start extends State
  case object Oldest extends State
  case object Younger extends State
  case object BecomingOldest extends State
  case object WasOldest extends State
  case object HandingOver extends State
  case object TakeOver extends State
  case object Stopping extends State
  case object End extends State

  var state: State = Start


  def handleOldestChanged(oldestOption: OldestChanged) = {
    if (state == Younger) {
      handleAsYounger(oldestOption)
    } else if (state == Oldest) {
      handleAsOldest(oldestOption)
    }
  }



  def handleAsOldest(oldestOption: OldestChanged) = {
    if (oldestOption.oldest == selfUniqueAddress) {
      //as is continue
    } else {
      //someone else is oldest.
    }
  }

  private def handleAsYounger(oldestOption: OldestChanged) = {
    if (oldestOption.oldest == selfUniqueAddress) {
      // oldest immediately
      state = Oldest
      //need to figure out if there was a different older previously
      gotoOldest()
    } else {
      state = Younger
    }
  }

  def publish(memberEvent: MemberEvent) = {
    memberEvent match {
      case MemberUp(m) => {
        if (m.address.eq(selfUniqueAddress)) {
          startHeartbeating()
        } else {
          heartbeat.addMember(m)
        }
        val before = membersByAge.headOption
        // replace, it's possible that the upNumber is changed
        membersByAge = membersByAge.filterNot(_.uniqueAddress == m.uniqueAddress)
        membersByAge += m
        val after = membersByAge.headOption
        if (before != after) {
          changes :+= OldestChanged(after.map(_.uniqueAddress))
          handleOldestChanged(changes.head)
          changes = changes.tail
        }
      };
      case MemberRemoved(m, previousStatus) =>
      case _: MemberEvent => // ignore
    }
  }

  val q = new SingularUpdateQueue[Message, Message]((message) => {
    handleReceive(message)
    message
  })
  q.start()

  def leaderActionsOnConvergence() = {
    info("Leader actions on convergence in " + selfUniqueAddress)
    val localGossip = latestGossip
    val localMembers = localGossip.members
    val localOverview = localGossip.overview
    val localSeen = localOverview.seen

    val removedUnreachable = for {
      node ← localOverview.reachability.allUnreachableOrTerminated
      m = localGossip.member(node)
      if Gossip.removeUnreachableWithMemberStatus(m.status)
    } yield m

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
      //      val newMembers = changedMembers union localMembers diff removedUnreachable
      //FIXME: in akka 2.4
      //      val newMembers = localMembers union changedMembers diff removedUnreachable

      // replace changed members
      //      val removed = removedUnreachable
      //        .map(_.uniqueAddress)
      //        .union(removedExitingConfirmed)
      //        .union(removedOtherDc.map(_.uniqueAddress))

      val newGossip = latestGossip.update(changedMembers).removeAll(removedUnreachable.map(_.uniqueAddress)
        , System.currentTimeMillis())

      val oldMembershipState = updateLatestGossip(newGossip) //updates the membershipstate and
      publishChanges(oldMembershipState, membershipState)
      // log status changes
      changedMembers foreach { m ⇒
        info(s"Leader ${selfUniqueAddress} is moving node [${m.address}] to [${m.status}]")
      }
      info(s"$membershipState")
    }
  }


  val MinNrOfMembers = 2

  def isMinNrOfMembersFulfilled: Boolean = {
    latestGossip.members.size >= MinNrOfMembers
  }

  def leaderActions() = {
    info("doing leader actions in " + selfUniqueAddress)
    if (membershipState.isLeader(selfUniqueAddress)) {
      info(s"${selfUniqueAddress} is leader in ${membershipState}")
      if (!isCurrentlyLeader) {
        info(s"${selfUniqueAddress} is the new leader among reachable nodes (more leaders may exist)")
        isCurrentlyLeader = true
      }
      if (membershipState.convergence(exitingConfirmed)) {
        leaderActionCounter = 0
        leaderActionsOnConvergence()
      } else {

      }
    } else {
      info(s"${selfUniqueAddress} is not leader in ${membershipState}")
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
        addJoiningMember(joiningNode)

        if (joiningNode == selfUniqueAddress) {
          info(
            s"Node [${joiningNode}] is JOINING itself forming new cluster")
          if (localMembers.isEmpty) {}
          //            leaderActions() //TODO: Figure out why this is important for deterministic oldest when bootstrapping
        } else {
          info(
            s"Node [${joiningNode}] is JOINING")
          networkIO.send(joiningNode, Welcome(selfUniqueAddress, joiningNode, latestGossip))
        }
    }
  }

  def addJoiningMember(joiningNode: InetAddressAndPort) = {
    val localMembers = latestGossip.members
    val newMembers = localMembers + Member(joiningNode) + Member(selfUniqueAddress)
    val newGossip = latestGossip.copy(members = newMembers)

    updateLatestGossip(newGossip)

  }

  def gossipTo(node: InetAddressAndPort): Unit =
    if (membershipState.validNodeForGossip(node)) {
      info(s"Gossiping from ${selfUniqueAddress}to ${node}")
      networkIO.send(node, GossipEnvelope(selfUniqueAddress, node, latestGossip))
    }
}


