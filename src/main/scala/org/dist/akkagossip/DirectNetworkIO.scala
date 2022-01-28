package org.dist.akkagossip

import org.dist.patterns.common.InetAddressAndPort

class Message(val from:InetAddressAndPort){}


class DirectNetworkIO(var connections:Map[InetAddressAndPort, ClusterDaemon]) {
  val disconnections:scala.collection.mutable.Map[InetAddressAndPort, InetAddressAndPort] = scala.collection.mutable.Map()

  def disconnect(from:InetAddressAndPort, to:InetAddressAndPort): Unit ={
    disconnections.put(from, to)
  }

  def send(node: InetAddressAndPort, message: Message):Unit = {
    //drop message if disconnection added.
    if (hasDisconnectionFor(message, node)) {
      return
    }

    connections.get(node) match {
      case Some(connection) => connection.receive(message)
      case None =>
    }
  }

  private def hasDisconnectionFor(message: Message, toAddress: InetAddressAndPort) = {
    !disconnections.isEmpty &&
    !disconnections.get(message.from).isEmpty &&
      disconnections.get(message.from).get == toAddress
  }
}
