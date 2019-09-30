package org.dist.kvstore.client

import org.dist.kvstore._
import org.dist.util.Networks

class Client(bootstrapServer: InetAddressAndPort) {
  private val socketClient = new SocketClient
  def put(table: String, key: String, value: String) = {
    val mutation = RowMutation(table, key, value)

    val header = Header(InetAddressAndPort(new Networks().ipv4Address, 8000)
      , Stage.MUTATION, Verb.ROW_MUTATION)
    val message = Message(header, JsonSerDes.serialize(mutation))
    socketClient.sendReceiveTcp(message, bootstrapServer)
  }
}
