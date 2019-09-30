package org.dist.kvstore

import java.net.{InetSocketAddress, ServerSocket}
import java.util

import org.slf4j.LoggerFactory

class StorageProxy(clientRequestIp: InetAddressAndPort, storageService: StorageService) {

  def start(): Unit = {
      new TcpClientRequestListner(clientRequestIp, storageService).start()
  }
}


class TcpClientRequestListner(localEp: InetAddressAndPort, storageService:StorageService) extends Thread {
  private[kvstore] val logger = LoggerFactory.getLogger(classOf[TcpListener])

  override def run(): Unit = {
    val serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress(localEp.address, localEp.port))
    println(s"Listening on ${localEp}")
    while (true) {
      val socket = serverSocket.accept()
      socket.setSoTimeout(1000)
      val inputStream = socket.getInputStream()
      val messageBytes = inputStream.readAllBytes()

      val message = JsonSerDes.deserialize(messageBytes, classOf[Message])

      logger.debug(s"Got message ${message}")

      if (message.header.verb == Verb.ROW_MUTATION) {
        new RowMutationHandler(storageService)
      }

      inputStream.close()
      socket.close()
    }
  }

  class RowMutationHandler(storageService: StorageService) {
    def handleMessage(rowMutationMessage: Message) = {
      val rowMutation = JsonSerDes.deserialize(rowMutationMessage.payloadJson.getBytes, classOf[RowMutation])
      val serversHostingKey = storageService.getNStorageEndPointMap(rowMutation.key)
    }
  }

}
