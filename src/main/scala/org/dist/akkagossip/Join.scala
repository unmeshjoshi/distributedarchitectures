package org.dist.akkagossip

import org.dist.patterns.common.InetAddressAndPort

case class Join(fromAddress: InetAddressAndPort) extends Message(fromAddress) {

}
