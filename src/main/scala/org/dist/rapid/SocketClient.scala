package org.dist.rapid

import java.net.Socket

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.api.RequestOrResponse
import org.dist.util.SocketIO

class SocketClient {
  def sendReceive(requestOrResponse: RequestOrResponse, to:InetAddressAndPort):RequestOrResponse = {
    val clientSocket = new Socket(to.address, to.port)
    new SocketIO[RequestOrResponse](clientSocket, classOf[RequestOrResponse]).requestResponse(requestOrResponse)
  }

  def sendOneWay(requestOrResponse: RequestOrResponse, to:InetAddressAndPort):Unit = {
    val clientSocket = new Socket(to.address, to.port)
    new SocketIO[RequestOrResponse](clientSocket, classOf[RequestOrResponse]).write(requestOrResponse)
  }
}
