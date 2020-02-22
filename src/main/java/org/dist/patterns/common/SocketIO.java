package org.dist.patterns.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.function.BiFunction;

public class SocketIO<T> {
    private Socket clientSocket;
    private Class<T> responseClass;

    public SocketIO(Socket clientSocket, Class<T> responseClass) {
        this.clientSocket = clientSocket;
        this.responseClass = responseClass;
    }

    public void write(T message) {
        write(clientSocket, JsonSerDes.serialize(message));
    }

    public void write(Socket socket, String serializedMessage) {
        try {
            var outputStream = socket.getOutputStream();
            var dataStream = new DataOutputStream(outputStream);
            var messageBytes = serializedMessage.getBytes();
            dataStream.writeInt(messageBytes.length);
            dataStream.write(messageBytes);
            outputStream.flush();

        } catch (IOException e) {
            new RuntimeException(e);
        }
    }

    public T readHandleWithSocket(BiFunction<T, Socket, T> handler) {
        var responseBytes = read(clientSocket);
        var message = JsonSerDes.deserialize(responseBytes, responseClass);
        return handler.apply(message, clientSocket);
    }

    public T read() {
      var responseBytes = read(clientSocket);
      return JsonSerDes.deserialize(responseBytes, responseClass);
    }

    private byte[] read(Socket socket) {
        try {
            var inputStream = socket.getInputStream();
            var dataInputStream = new DataInputStream(inputStream);
            var size = dataInputStream.readInt();
            var responseBytes = new byte[size];
            dataInputStream.read(responseBytes);
            return responseBytes;
            
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }

    public T requestResponse(T requestOrResponse) {
        write(clientSocket, JsonSerDes.serialize(requestOrResponse));
        var responseBytes = read(clientSocket);
        return JsonSerDes.deserialize(responseBytes, responseClass);
    }
}
