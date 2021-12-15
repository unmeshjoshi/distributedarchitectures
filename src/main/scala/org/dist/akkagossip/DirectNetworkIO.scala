package org.dist.akkagossip

import org.dist.patterns.common.InetAddressAndPort

trait Message


class DirectNetworkIO() {
  var connections:Map[InetAddressAndPort, ClusterDaemon] = Map()
  def send(node: InetAddressAndPort, message: Message) = {
    connections.get(node) match {
      case Some(connection) => connection.receive(message)
      case None =>
    }
  }

}
