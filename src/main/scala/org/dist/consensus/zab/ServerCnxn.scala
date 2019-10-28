package org.dist.consensus.zab

import java.net.{InetSocketAddress, ServerSocket, Socket}

import org.dist.consensus.zab.api.ClientRequestOrResponse
import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.common.Logging
import org.dist.queue.utils.Utils
import org.dist.util.SocketIO

//handles client communication with zk
class ServerCnxn(serverAddress:InetAddressAndPort, val zk:LeaderZookeeperServer) extends Thread with Logging {
  var serverSocket: ServerSocket = null

  override def run() = {
    listen()
  }

  def listen(): Unit = {
    Utils.swallow({
      serverSocket = new ServerSocket()
      serverSocket.bind(new InetSocketAddress(serverAddress.address, serverAddress.port))
      info(s"Listening on ${serverAddress}")
      while (true) {
        val socket: Socket = serverSocket.accept()
        new SocketIO(socket, classOf[ClientRequestOrResponse]).readHandleWithSocket((request, clientSocket) ⇒ {
          zk.submitRequest(request, clientSocket)
        })
      }
    }
    )
  }
}
