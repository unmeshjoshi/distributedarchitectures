package org.dist.akkagossip

import org.dist.patterns.common.InetAddressAndPort

import scala.collection.immutable
import Member._
import org.dist.akkagossip.MemberStatus.{Down, Exiting}

/**
 * INTERNAL API
 */
object Gossip {
  val removeUnreachableWithMemberStatus = Set[MemberStatus](Down, Exiting)


  type Timestamp = Long
  val emptyMembers: immutable.SortedSet[Member] = immutable.SortedSet.empty
  val empty: Gossip = new Gossip(Gossip.emptyMembers)

  def apply(members: immutable.SortedSet[Member]): Gossip =
    if (members.isEmpty) empty else empty.copy(members = members)

  def vclockName(node: InetAddressAndPort): String = s"${node.getAddress}"

}
case class Gossip(
     members: immutable.SortedSet[Member], // sorted set of members with their status, sorted by address
     overview: GossipOverview = GossipOverview(),
     version: VectorClock = VectorClock(), // vector clock version
     tombstones: Map[InetAddressAndPort, Gossip.Timestamp] = Map.empty) {


  /**
   * Remove the given member from the set of members and mark it's removal with a tombstone to avoid having it
   * reintroduced when merging with another gossip that has not seen the removal.
   */
  def remove(node: InetAddressAndPort, removalTimestamp: Long): Gossip = {
    // removing REMOVED nodes from the `seen` table
    val newSeen = overview.seen - node
    // removing REMOVED nodes from the `reachability` table
    val newReachability = overview.reachability.remove(node :: Nil)
    val newOverview = overview.copy(seen = newSeen, reachability = newReachability)

    // Clear the VectorClock when member is removed. The change made by the leader is stamped
    // and will propagate as is if there are no other changes on other nodes.
    // If other concurrent changes on other nodes (e.g. join) the pruning is also
    // taken care of when receiving gossips.
    val newVersion = version.prune(VectorClock.Node(Gossip.vclockName(node)))
    val newMembers = members.filterNot(_.uniqueAddress == node)
    val newTombstones = tombstones + (node -> removalTimestamp)
    copy(version = newVersion, members = newMembers, overview = newOverview, tombstones = newTombstones)
  }

  def removeAll(nodes: Iterable[InetAddressAndPort], removalTimestamp: Long): Gossip = {
    nodes.foldLeft(this)((gossip, node) => gossip.remove(node, removalTimestamp))
  }


  def hasMember(node: InetAddressAndPort): Boolean = membersMap.contains(node)

  val isSingletonCluster: Boolean = members.size == 1


  def update(updatedMembers: immutable.SortedSet[Member]): Gossip = {
    copy(members = updatedMembers.union(members.diff(updatedMembers)))
  }

  /**
   * Marks the gossip as seen by only this node (address) by replacing the 'gossip.overview.seen'
   */
  def onlySeen(node: InetAddressAndPort): Gossip = {
    this.copy(overview = overview.copy(seen = Set(node)))
  }


  /**
   * Remove all seen entries
   */
  def clearSeen(): Gossip = {
    this.copy(overview = overview.copy(seen = Set.empty))
  }


  @transient private lazy val membersMap: Map[InetAddressAndPort, Member] =
    members.iterator.map(m => m.uniqueAddress -> m).toMap

  def member(node: InetAddressAndPort): Member = {
    membersMap.getOrElse(node, Member.removed(node)) // placeholder for removed member
  }



  def seen(node: InetAddressAndPort): Gossip = {
    if (seenByNode(node)) this
    else this.copy(overview = overview.copy(seen = overview.seen + node))
  }

  /**
   * Merges the seen table of two Gossip instances.
   */
  def mergeSeen(that: Gossip): Gossip =
    this.copy(overview = overview.copy(seen = overview.seen.union(that.overview.seen)))

  def seenByNode(address: InetAddressAndPort) = overview.seen(address)

  /**
   * Increments the version for this 'Node'.
   */
  def :+(node: VectorClock.Node): Gossip = copy(version = version :+ node)

  /**
   * Adds a member to the member node ring.
   */
  def :+(member: Member): Gossip = {
    if (members contains member) this
    else this.copy(members = members + member)
  }
}
