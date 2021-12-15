package org.dist.akkagossip

import org.dist.patterns.common.InetAddressAndPort

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

case class GossipOverview(
   seen: Set[InetAddressAndPort] = Set.empty,
   reachability: Reachability = Reachability.empty) {

  lazy val seenDigest: Array[Byte] = {
    val bytes = seen.toVector.sorted.map(node => node.getAddress).mkString(",").getBytes(StandardCharsets.UTF_8)
    MessageDigest.getInstance("SHA-1").digest(bytes)
  }

  override def toString =
    s"GossipOverview(reachability = [$reachability], seen = [${seen.mkString(", ")}])"
}
