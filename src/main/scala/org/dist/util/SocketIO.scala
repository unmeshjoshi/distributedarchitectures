package org.dist.util

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket

import org.dist.kvstore.JsonSerDes

import scala.util.Using

class SocketIO[T](clientSocket: Socket, responseClass: Class[T]) {
  clientSocket.setSoTimeout(5000)

  def readHandleWithSocket(handler:(T, Socket) => Any): Unit = {
    val responseBytes = read(clientSocket)
    val message = JsonSerDes.deserialize(responseBytes, responseClass)
    handler(message, clientSocket)
  }

  def readHandleRespond(handler:T => Any): Unit = {
    Using.resource(clientSocket) { socket =>
      val responseBytes = read(socket)
      val message = JsonSerDes.deserialize(responseBytes, responseClass)
      val response = handler(message)
      write(socket, JsonSerDes.serialize(response))
    }
  }

  def read(): T = {
    Using.resource(clientSocket) { socket =>
      val responseBytes = read(socket)
      JsonSerDes.deserialize(responseBytes, responseClass)
    }
  }

  def write(message:T):Unit = {
    Using.resource(clientSocket) { socket =>
      write(socket, JsonSerDes.serialize(message))
    }
  }

  private def read(socket: Socket) = {
    val inputStream = socket.getInputStream
    val dataInputStream = new DataInputStream(inputStream)
    val size = dataInputStream.readInt()
    val responseBytes = new Array[Byte](size)
    dataInputStream.read(responseBytes)
    responseBytes
  }

  def requestResponse(requestMessage: T): T = {
    Using.resource(clientSocket) { socket =>
      write(socket, JsonSerDes.serialize(requestMessage))
      val responseBytes: Array[Byte] = read(socket)
      JsonSerDes.deserialize(responseBytes, responseClass)
    }
  }

  private def write(socket: Socket, serializedMessage: String) = {
    val outputStream = socket.getOutputStream()
    val dataStream = new DataOutputStream(outputStream)
    val messageBytes = serializedMessage.getBytes
    dataStream.writeInt(messageBytes.size)
    dataStream.write(messageBytes)
    outputStream.flush()
  }
}
