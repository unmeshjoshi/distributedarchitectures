package org.dist.akkagossip

import org.dist.patterns.common.InetAddressAndPort

case class Welcome(override val from: InetAddressAndPort, joiningWith: InetAddressAndPort, latestGossip: Gossip) extends Message(from) {

}
