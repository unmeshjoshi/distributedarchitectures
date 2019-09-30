package org.dist.kvstore.client

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes, Message}

class SocketClient {


  def sendReceiveTcp(message: Message, to: InetAddressAndPort) = {
    val clientSocket = new Socket(to.address, to.port)
    clientSocket.setSoTimeout(1000)
    try {
      val serializedMessage = JsonSerDes.serialize(message)

      val outputStream = clientSocket.getOutputStream()
      val dataStream = new DataOutputStream(outputStream)

      val messageBytes = serializedMessage.getBytes
      dataStream.writeInt(messageBytes.size)
      dataStream.write(messageBytes)
      outputStream.flush()


      val inputStream = clientSocket.getInputStream
      val dataInputStream = new DataInputStream(inputStream)
      //
      val size = dataInputStream.readInt()
      val responseBytes = new Array[Byte](size)
      dataInputStream.read(responseBytes)

      val response = JsonSerDes.deserialize(responseBytes, classOf[Message])

      println("received response " + response)
      outputStream.close()
      response


    } catch {

      case e: Exception => throw new RuntimeException(e)

    } finally {
      clientSocket.close()
    }
  }
}
