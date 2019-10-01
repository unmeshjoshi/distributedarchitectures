package org.dist.kvstore

import java.io.DataInputStream
import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.util

import org.slf4j.LoggerFactory

class StorageProxy(clientRequestIp: InetAddressAndPort, storageService: StorageService) {

  def start(): Unit = {
      new TcpClientRequestListner(clientRequestIp, storageService, this).start()
  }



  def sendTcpOneWay(message: Message, to: InetAddressAndPort) = {
    val clientSocket = new Socket(to.address, to.port)
    clientSocket.setSoTimeout(1000)
    try {
      val serializedMessage = JsonSerDes.serialize(message)
      val outputStream = clientSocket.getOutputStream()
      outputStream.write(serializedMessage.getBytes)
      outputStream.flush()
      outputStream.close()

    } catch {

      case e:Exception => e.printStackTrace()

    } finally {
      clientSocket.close()
    }
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
      socket.setSoTimeout(1000)
      val inputStream = socket.getInputStream()
      val dataInputStream = new DataInputStream(inputStream)
      //
      val size = dataInputStream.readInt()
      val messageBytes = new Array[Byte](size)
      dataInputStream.read(messageBytes)

      val message = JsonSerDes.deserialize(messageBytes, classOf[Message])

      logger.debug(s"Got client message ${message}")

      if (message.header.verb == Verb.ROW_MUTATION) {
        new RowMutationHandler(storageService).handleMessage(message)
      }

      inputStream.close()
      socket.close()
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
