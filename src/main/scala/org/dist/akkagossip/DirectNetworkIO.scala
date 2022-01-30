package org.dist.akkagossip

import org.dist.patterns.common.InetAddressAndPort

class Message(val from:InetAddressAndPort){}


class DirectNetworkIO(var connections:Map[InetAddressAndPort, ClusterDaemon]) {
  val disconnections:scala.collection.mutable.Map[InetAddressAndPort, List[InetAddressAndPort]] = scala.collection.mutable.Map()

  def disconnect(fromNodes:List[InetAddressAndPort], toNodes:List[InetAddressAndPort]) = {
    this.synchronized {
      fromNodes.foreach(fromNode => {
        toNodes.foreach(toNode => {
          val disconnectedNodes = disconnections.get(fromNode) match {
            case Some(nodes) => nodes :+ toNode
            case None => List(toNode)
          }
          disconnections.put(fromNode, disconnectedNodes)
        })
        println(s"Disconnecting $fromNode to ${disconnections.get(fromNode).getOrElse(List())}")

      })
    }
  }

  def disconnect(from:InetAddressAndPort, to:InetAddressAndPort): Unit ={
    this.synchronized {
      disconnections.put(from, List(to))
    }
  }

  def send(node: InetAddressAndPort, message: Message):Unit = {
    this.synchronized {
      //drop message if disconnection added.
      if (hasDisconnectionFor(message, node)) {
        return
      }

      connections.get(node) match {
        case Some(connection) => connection.receive(message)
        case None =>
      }
    }
  }

  private def hasDisconnectionFor(message: Message, toAddress: InetAddressAndPort) = {
    !disconnections.isEmpty &&
      !disconnections.get(message.from).isEmpty &&
      disconnections.get(message.from).get.contains(toAddress)
  }
}
