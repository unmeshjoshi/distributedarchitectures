package org.dist.akkagossip

import org.dist.akkagossip.MemberStatus.{Down, Exiting, Leaving, Up}
import org.dist.patterns.common.InetAddressAndPort

import scala.collection.immutable

case class MembershipState(latestGossip: Gossip, selfUniqueAddress: InetAddressAndPort) {

  def allMembersUp(clusterSize: Int) = {
    members.size == clusterSize && members.forall(m => m.status == MemberStatus.Up)
  }

  def youngestMember: Member = {
    require(members.nonEmpty, "No youngest when no members")
    members.maxBy(m ⇒ if (m.upNumber == Int.MaxValue) 0 else m.upNumber)
  }

  def convergence(exitingConfirmed: Set[InetAddressAndPort]): Boolean = {
    // First check that:
    //   1. we don't have any members that are unreachable, excluding observations from members
    //      that have status DOWN, or
    //   2. all unreachable members in the set have status DOWN or EXITING
    // Else we can't continue to check for convergence
    // When that is done we check that all members with a convergence
    // status is in the seen table, i.e. has seen this version
//    val unreachable = reachabilityExcludingDownedObservers.allUnreachableOrTerminated.collect {
//      case node if (node != selfUniqueAddress) ⇒ latestGossip.member(node)
//    }
//    unreachable.forall(m ⇒ convergenceSkipUnreachableWithMemberStatus(m.status)) &&
//      !members.exists(m ⇒ convergenceMemberStatus(m.status) && !latestGossip.seenByNode(m.uniqueAddress))

    !members.exists(m ⇒ !latestGossip.seenByNode(m.uniqueAddress))
  }

  lazy val reachabilityExcludingDownedObservers: Reachability = {
    val downed = members.collect { case m if m.status == Down ⇒ m }
    overview.reachability.removeObservers(downed.map(_.uniqueAddress))

  }

  lazy val dcReachabilityExcludingDownedObservers: Reachability = {
    val membersToExclude = members.collect { case m if m.status == Down => m.uniqueAddress }
    overview.reachability
      .removeObservers(membersToExclude)
//      .remove(members.collect { case m if m.dataCenter != selfDc => m.uniqueAddress })
  }


  private val convergenceMemberStatus = Set[MemberStatus](Up, Leaving)
  private val convergenceSkipUnreachableWithMemberStatus = Set[MemberStatus](Down, Exiting)

  def isLeader(node: InetAddressAndPort): Boolean =
    leader.contains(node)

  def leader: Option[InetAddressAndPort] =
    leaderOf(members, selfUniqueAddress)

  def members: immutable.SortedSet[Member] = latestGossip.members

  lazy val dcReachability: Reachability =
    overview.reachability

  val leaderMemberStatus = Set[MemberStatus](Up, Leaving)
  private def leaderOf(mbrs: immutable.SortedSet[Member], selfUniqueAddress: InetAddressAndPort): Option[InetAddressAndPort] = {
    val reachableMembers =
      if (overview.reachability.isAllReachable) mbrs.filterNot(_.status == Down)
      else mbrs.filter(m ⇒ m.status != Down &&
        (overview.reachability.isReachable(m.uniqueAddress) || m.uniqueAddress == selfUniqueAddress))
    if (reachableMembers.isEmpty) None
    else reachableMembers.find(m ⇒ leaderMemberStatus(m.status)).
      orElse(Some(reachableMembers.min(Member.leaderStatusOrdering))).map(_.uniqueAddress)

  }


  lazy val selfMember = latestGossip.member(selfUniqueAddress)

  def seen(): MembershipState = copy(latestGossip = latestGossip.seen(selfUniqueAddress))

  def overview: GossipOverview = latestGossip.overview

  def validNodeForGossip(node: InetAddressAndPort): Boolean =
    node != selfUniqueAddress && overview.reachability.isReachable(selfUniqueAddress, node)
}
