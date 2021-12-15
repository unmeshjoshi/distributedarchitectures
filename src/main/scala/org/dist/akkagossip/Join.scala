package org.dist.akkagossip

import org.dist.patterns.common.InetAddressAndPort

case class Join(address: InetAddressAndPort) extends Message {

}
