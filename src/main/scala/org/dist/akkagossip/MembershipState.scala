package org.dist.akkagossip

import org.dist.akkagossip.MemberStatus.{Down, Exiting, Joining, Leaving, Up, WeaklyUp}
import org.dist.patterns.common.InetAddressAndPort

import scala.collection.immutable

case class MembershipState(latestGossip: Gossip, selfUniqueAddress: InetAddressAndPort) {
  def youngestMember: Member = {
    require(members.nonEmpty, "No youngest when no members")
    members.maxBy(m ⇒ if (m.upNumber == Int.MaxValue) 0 else m.upNumber)
  }

  def convergence(exitingConfirmed: Set[InetAddressAndPort]): Boolean = {
    // full convergence needed for first member in a secondary DC
   val firstMemberInDc = !members.exists(member => convergenceMemberStatus(member.status))

    // If another member in the data center that is UP or LEAVING and has not seen this gossip or is exiting
    // convergence cannot be reached. For the first member in a secondary DC all Joining, WeaklyUp, Up or Leaving
    // members must have seen the gossip state. The reason for the stronger requirement for a first member in a
    // secondary DC is that first member should only be moved to Up once to ensure that the first upNumber is
    // only assigned once.
    val memberHinderingConvergenceExists = {
      val memberStatus = if (firstMemberInDc) convergenceMemberStatus + Joining + WeaklyUp else convergenceMemberStatus
      members.exists(
        member =>
          (firstMemberInDc) &&
            memberStatus(member.status) &&
            !(latestGossip.seenByNode(member.uniqueAddress) || exitingConfirmed(member.uniqueAddress)))
    }
    // Find cluster members in the data center that are unreachable from other members of the data center
    // excluding observations from members outside of the data center, that have status DOWN or is passed in as confirmed exiting.
    val unreachableInDc = dcReachabilityExcludingDownedObservers.allUnreachableOrTerminated.collect {
      case node if node != selfUniqueAddress && !exitingConfirmed(node) => latestGossip.member(node)
    }

    // unreachables outside of the data center or with status DOWN or EXITING does not affect convergence
    val allUnreachablesCanBeIgnored =
      unreachableInDc.forall(unreachable => convergenceSkipUnreachableWithMemberStatus(unreachable.status))

    allUnreachablesCanBeIgnored && !memberHinderingConvergenceExists
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
