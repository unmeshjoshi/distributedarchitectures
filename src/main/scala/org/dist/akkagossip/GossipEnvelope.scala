package org.dist.akkagossip

import org.dist.patterns.common.InetAddressAndPort

case class GossipEnvelope(override val from: InetAddressAndPort,
                          to: InetAddressAndPort,
                          latestGossip: Gossip) extends Message(from) {

}
