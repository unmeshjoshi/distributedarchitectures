package org.dist.dbgossip;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class MessagingService {
    public static MessagingService instance = new MessagingService();
    public static MessagingService instance() {
        return instance;
    }

    public void send(Message message, InetAddressAndPort to) {

    }

    public void waitUntilListening() {

    }

    public void listen() {
        try {
            ServerSocket serverSocket = new ServerSocket(9000);
            Socket accept = serverSocket.accept();
            InputStream inputStream = accept.getInputStream();
            int length = inputStream.read();
            byte[] bytes = inputStream.readNBytes(length);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
