package org.dist.rapid.messages

import org.dist.kvstore.InetAddressAndPort

object AlertMessage {
  val UP = 0
  val DOWN = 1
}

case class AlertMessage(status:Int, address:InetAddressAndPort)
