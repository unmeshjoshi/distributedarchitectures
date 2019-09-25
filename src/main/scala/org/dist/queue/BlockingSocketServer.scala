package org.dist.queue

import java.net.{InetSocketAddress, ServerSocket, Socket}

import org.dist.kvstore._

/**
 * For handling requests
 */
class BlockingSocketServer {


  def listen(localEp: InetAddressAndPort): Unit = {
    new TcpListener(localEp, this).start()
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

      case e: Exception => e.printStackTrace()

    } finally {
      clientSocket.close()
    }
  }

  class TcpListener(localEp: InetAddressAndPort, messagingService: BlockingSocketServer) extends Thread {

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

        println(s"Got message ${message}")

        inputStream.close()
        socket.close()
      }
    }
  }
}
