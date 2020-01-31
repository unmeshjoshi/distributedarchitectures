package org.dist.patterns.replicatedlog

import java.net.{InetSocketAddress, ServerSocket}
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.patterns.replicatedlog.api.{RequestKeys, VoteResponse}
import org.dist.queue.api.RequestOrResponse
import org.dist.queue.common.Logging
import org.dist.queue.utils.Utils
import org.dist.util.SocketIO


class SingularUpdateQueue(handler:RequestOrResponse => RequestOrResponse) extends Thread {
  val workQueue = new ArrayBlockingQueue[(RequestOrResponse, SocketIO[RequestOrResponse])](100)
  @volatile var running = true

  def shutdown() = {
    running = false
  }

  def submitRequest(req: RequestOrResponse, socketIo:SocketIO[RequestOrResponse]): Unit = {
    workQueue.add((req, socketIo))
  }

  override def run(): Unit = {
    while (running) {
      val (request, socketIo) = workQueue.take()
      val response = handler(request)
      socketIo.write(response)
    }
  }

}


class TcpListener(localEp: InetAddressAndPort, server: Server) extends Thread with Logging {
  val isRunning = new AtomicBoolean(true)
  var serverSocket: ServerSocket = null

  def shudown() = {
    Utils.swallow(serverSocket.close())
  }

  val workQueue = new SingularUpdateQueue((request:RequestOrResponse) ⇒ {
    if (request.requestId == RequestKeys.RequestVoteKey) {
      val vote = VoteResponse(server.currentVote.get().id, server.currentVote.get().zxid)
      info(s"Responding vote response from ${server.myid} be ${server.currentVote}")
      RequestOrResponse(RequestKeys.RequestVoteKey, JsonSerDes.serialize(vote), request.correlationId)


    } else if (request.requestId == RequestKeys.AppendEntriesKey) {

      val appendEntries = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[AppendEntriesRequest])
      val appendEntriesResponse = server.handleAppendEntries(appendEntries)
      info(s"Responding AppendEntriesResponse from ${server.myid} be ${appendEntriesResponse}")
      RequestOrResponse(RequestKeys.AppendEntriesKey, JsonSerDes.serialize(appendEntriesResponse), request.correlationId)

    } else throw new RuntimeException("UnknownRequest")

  })

  workQueue.start()

  override def run(): Unit = {
    Utils.swallow({
      serverSocket = new ServerSocket()
      serverSocket.bind(new InetSocketAddress(localEp.address, localEp.port))
      info(s"Listening on ${localEp}")
      while (true) {
        val socket = serverSocket.accept()
        val socketIo = new SocketIO(socket, classOf[RequestOrResponse])
        socketIo.readHandleWithSocket((request, socket) ⇒ { //Dont close socket after read. It will be closed after write
          workQueue.submitRequest(request, socketIo)
        })
      }
    }
    )
  }
}