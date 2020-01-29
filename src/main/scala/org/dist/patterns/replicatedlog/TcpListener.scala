package org.dist.patterns.replicatedlog

import java.net.{InetSocketAddress, ServerSocket}
import java.util.concurrent.atomic.AtomicBoolean

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.patterns.replicatedlog.api.{RequestKeys, VoteResponse}
import org.dist.queue.api.RequestOrResponse
import org.dist.queue.common.Logging
import org.dist.queue.utils.Utils
import org.dist.util.SocketIO


class TcpListener(localEp: InetAddressAndPort, server: Server) extends Thread with Logging {
  val isRunning = new AtomicBoolean(true)
  var serverSocket: ServerSocket = null

  def shudown() = {
    Utils.swallow(serverSocket.close())
  }

  override def run(): Unit = {
    Utils.swallow({
      serverSocket = new ServerSocket()
      serverSocket.bind(new InetSocketAddress(localEp.address, localEp.port))
      info(s"Listening on ${localEp}")
      while (true) {
        val socket = serverSocket.accept()
        new SocketIO(socket, classOf[RequestOrResponse]).readHandleRespond((request) ⇒ {
          if (request.requestId == RequestKeys.RequestVoteKey) {
            val vote = VoteResponse(server.currentVote.get().id, server.currentVote.get().zxid)
            info(s"Responding vote response from ${server.myid} be ${server.currentVote}")
            RequestOrResponse(RequestKeys.RequestVoteKey, JsonSerDes.serialize(vote), request.correlationId)
          } else throw new RuntimeException("UnknownRequest")
        })
      }
    }
    )
  }
}