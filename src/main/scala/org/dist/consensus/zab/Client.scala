package org.dist.consensus.zab

import java.net.Socket
import java.util.concurrent.atomic.AtomicInteger

import org.dist.consensus.zab.api.{ClientRequestOrResponse, SetDataRequest}
import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.util.SocketIO

class Client(to:InetAddressAndPort) {
  val correlationId = new AtomicInteger(0)

  def setData(path:String, data:String) = {
    val clientSocket = new Socket(to.address, to.port)
    val request = SetDataRequest(path, data)
    val setDataRequest = ClientRequestOrResponse(ClientRequestOrResponse.SetDataKey, JsonSerDes.serialize(request), correlationId.getAndIncrement())
    new SocketIO[ClientRequestOrResponse](clientSocket, classOf[ClientRequestOrResponse]).requestResponse(setDataRequest)
  }
}
