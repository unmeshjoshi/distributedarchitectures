package org.dist.akkagossip

import org.dist.akkagossip.MemberStatus._
import org.dist.patterns.common.InetAddressAndPort

object Member {
  def apply(uniqueAddress: InetAddressAndPort): Member =
    new Member(uniqueAddress, Int.MaxValue, Joining)

  def removed(node: InetAddressAndPort): Member =
    new Member(node, Int.MaxValue, Removed)


  /**
   * `Address` ordering type class, sorts addresses by host and port.
   */
  implicit val addressOrdering: Ordering[InetAddressAndPort] = Ordering.fromLessThan[InetAddressAndPort] { (a, b) =>
    // cluster node identifier is the host and port of the address; protocol and system is assumed to be the same
    if (a eq b) false
    else if (a.getAddress != b.getAddress) a.compareTo(b) < 0
    else if (a.getPort != b.getPort) a.getPort < b.getPort
    else false
  }

  /**
   * INTERNAL API
   * Orders the members by their address except that members with status
   * Joining, Exiting and Down are ordered last (in that order).
   */
  val leaderStatusOrdering: Ordering[Member] = Ordering.fromLessThan[Member] { (a, b) =>
    (a.status, b.status) match {
      case (as, bs) if as == bs => ordering.compare(a, b) <= 0
      case (Down, _)            => false
      case (_, Down)            => true
      case (Exiting, _)         => false
      case (_, Exiting)         => true
      case (Joining, _)         => false
      case (_, Joining)         => true
      case (WeaklyUp, _)        => false
      case (_, WeaklyUp)        => true
      case _                    => ordering.compare(a, b) <= 0
    }
  }

  /**
   * `Member` ordering type class, sorts members by host and port.
   */
  implicit val ordering: Ordering[Member] = new Ordering[Member] {
    def compare(a: Member, b: Member): Int = {
      a.uniqueAddress.compareTo(b.uniqueAddress)
    }
  }

  /**
   * Sort members by age, i.e. using [[Member#isOlderThan]].
   *
   * Note that it only makes sense to compare with other members of
   * same data center. To avoid mistakes of comparing members of different
   * data centers it will throw `IllegalArgumentException` if the
   * members belong to different data centers.
   */
  val ageOrdering: Ordering[Member] = Ordering.fromLessThan[Member] { (a, b) =>
    a.isOlderThan(b)
  }
}

case class Member(val uniqueAddress: InetAddressAndPort,
             val upNumber: Int, // INTERNAL API
             val status: MemberStatus) {

  def copyUp(upNumber: Int): Member = {
    new Member(uniqueAddress, upNumber, status).copy(Up)
  }


  def address: InetAddressAndPort = uniqueAddress



  import Member._
  /**
   * Is this member older, has been part of cluster longer, than another
   * member. It is only correct when comparing two existing members in a
   * cluster. A member that joined after removal of another member may be
   * considered older than the removed member.
   *
   * Note that it only makes sense to compare with other members of
   * same data center (upNumber has a higher risk of being reused across data centers).
   * To avoid mistakes of comparing members of different data centers this
   * method will throw `IllegalArgumentException` if the members belong
   * to different data centers.
   */
  @throws[IllegalArgumentException]("if members from different data centers")
  def isOlderThan(other: Member): Boolean = {
//    if (dataCenter != other.dataCenter)
//      throw new IllegalArgumentException(
//        "Comparing members of different data centers with isOlderThan is not allowed. " +
//          s"[$this] vs. [$other]")
//    if (upNumber == other.upNumber)
      addressOrdering.compare(this.uniqueAddress, other.uniqueAddress) < 0
//    else
//      upNumber < other.upNumber
  }

  def copy(status: MemberStatus): Member = {
    val oldStatus = this.status
    if (status == oldStatus) this
    else {
      require(
        allowedTransitions(oldStatus)(status),
        s"Invalid member status transition [ ${this} -> ${status}]")
      new Member(uniqueAddress, upNumber, status)
    }
  }
}

/**
 * Defines the current status of a cluster member node
 *
 * Can be one of: Joining, WeaklyUp, Up, Leaving, Exiting and Down and Removed.
 */
sealed abstract class MemberStatus

object MemberStatus {
  @SerialVersionUID(1L) case object Joining extends MemberStatus
  @SerialVersionUID(1L) case object WeaklyUp extends MemberStatus
  @SerialVersionUID(1L) case object Up extends MemberStatus
  @SerialVersionUID(1L) case object Leaving extends MemberStatus
  @SerialVersionUID(1L) case object Exiting extends MemberStatus
  @SerialVersionUID(1L) case object Down extends MemberStatus
  @SerialVersionUID(1L) case object Removed extends MemberStatus

  /**
   * Java API: retrieve the `Joining` status singleton
   */
  def joining: MemberStatus = Joining

  /**
   * Java API: retrieve the `WeaklyUp` status singleton.
   */
  def weaklyUp: MemberStatus = WeaklyUp

  /**
   * Java API: retrieve the `Up` status singleton
   */
  def up: MemberStatus = Up

  /**
   * Java API: retrieve the `Leaving` status singleton
   */
  def leaving: MemberStatus = Leaving

  /**
   * Java API: retrieve the `Exiting` status singleton
   */
  def exiting: MemberStatus = Exiting

  /**
   * Java API: retrieve the `Down` status singleton
   */
  def down: MemberStatus = Down

  /**
   * Java API: retrieve the `Removed` status singleton
   */
  def removed: MemberStatus = Removed

  /**
   * INTERNAL API
   */
  val allowedTransitions: Map[MemberStatus, Set[MemberStatus]] =
    Map(
      Joining -> Set(WeaklyUp, Up, Leaving, Down, Removed),
      WeaklyUp -> Set(Up, Leaving, Down, Removed),
      Up -> Set(Leaving, Down, Removed),
      Leaving -> Set(Exiting, Down, Removed),
      Down -> Set(Removed),
      Exiting -> Set(Removed, Down),
      Removed -> Set.empty[MemberStatus])
}
