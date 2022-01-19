package org.dist.akkagossip

import org.dist.akkagossip.MemberStatus._

object ClusterEvent {

  sealed trait ClusterDomainEvent
  /**
   * Marker interface for membership events.
   * Published when the state change is first seen on a node.
   * The state change was performed by the leader when there was
   * convergence on the leader node, i.e. all members had seen previous
   * state.
   */
  sealed trait MemberEvent extends ClusterDomainEvent {
    def member: Member
  }

  /**
   * Member status changed to Joining.
   */
  final case class MemberJoined(member: Member) extends MemberEvent {
    if (member.status != Joining) throw new IllegalArgumentException("Expected Joining status, got: " + member)
  }

  /**
   * Member status changed to WeaklyUp.
   * A joining member can be moved to `WeaklyUp` if convergence
   * cannot be reached, i.e. there are unreachable nodes.
   * It will be moved to `Up` when convergence is reached.
   */
  final case class MemberWeaklyUp(member: Member) extends MemberEvent {
    if (member.status != WeaklyUp) throw new IllegalArgumentException("Expected WeaklyUp status, got: " + member)
  }

  /**
   * Member status changed to Up.
   */
  final case class MemberUp(member: Member) extends MemberEvent {
    if (member.status != Up) throw new IllegalArgumentException("Expected Up status, got: " + member)
  }

  /**
   * Member status changed to Leaving.
   */
  final case class MemberLeft(member: Member) extends MemberEvent {
    if (member.status != Leaving) throw new IllegalArgumentException("Expected Leaving status, got: " + member)
  }

  /**
   * Member status changed to `MemberStatus.Exiting` and will be removed
   * when all members have seen the `Exiting` status.
   */
  final case class MemberExited(member: Member) extends MemberEvent {
    if (member.status != Exiting) throw new IllegalArgumentException("Expected Exiting status, got: " + member)
  }

  /**
   * Member completely removed from the cluster.
   * When `previousStatus` is `MemberStatus.Down` the node was removed
   * after being detected as unreachable and downed.
   * When `previousStatus` is `MemberStatus.Exiting` the node was removed
   * after graceful leaving and exiting.
   */
  final case class MemberRemoved(member: Member, previousStatus: MemberStatus) extends MemberEvent {
    if (member.status != Removed) throw new IllegalArgumentException("Expected Removed status, got: " + member)
  }

}
