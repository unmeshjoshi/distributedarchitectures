package org.dist.kvstore

import java.net.{InetSocketAddress, ServerSocket, Socket}

import org.dist.util.SocketIO
import org.slf4j.LoggerFactory

class StorageProxy(clientRequestIp: InetAddressAndPort, storageService: StorageService) {

  def start(): Unit = {
      new TcpClientRequestListner(clientRequestIp, storageService, this).start()
  }

  def sendTcpOneWay(message: Message, to: InetAddressAndPort) = {
    val clientSocket = new Socket(to.address, to.port)
    new SocketIO[Message](clientSocket, classOf[Message]).write(message)
  }
}


class TcpClientRequestListner(localEp: InetAddressAndPort, storageService:StorageService, storageProxy:StorageProxy) extends Thread {
  private[kvstore] val logger = LoggerFactory.getLogger(classOf[TcpListener])

  override def run(): Unit = {
    val serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress(localEp.address, localEp.port))
    println(s"Listening for client connections on ${localEp}")
    while (true) {
      val socket = serverSocket.accept()
      val message = new SocketIO[Message](socket, classOf[Message]).read()

      logger.debug(s"Got client message ${message}")

      if (message.header.verb == Verb.ROW_MUTATION) {
        new RowMutationHandler(storageService).handleMessage(message)
      }

    }
  }

  class RowMutationHandler(storageService: StorageService) {
    def handleMessage(rowMutationMessage: Message) = {
      val rowMutation = JsonSerDes.deserialize(rowMutationMessage.payloadJson.getBytes, classOf[RowMutation])
      val serversHostingKey = storageService.getNStorageEndPointMap(rowMutation.key)
      serversHostingKey.foreach(endPoint ⇒ {
        storageProxy.sendTcpOneWay(rowMutationMessage, endPoint)
      })
    }
  }
}
