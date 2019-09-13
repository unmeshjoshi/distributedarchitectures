package org.dist.dbgossip;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class MessagingService {
    public static MessagingService instance = new MessagingService();
    public static MessagingService instance() {
        return instance;
    }

    private Listner listner = new Listner();

    public void send(Message message, InetAddressAndPort to) {
        requestQueue.add(message);
    }

    public void waitUntilListening() {
         listner.start();
         while(true) {
             if (serverSocket != null && serverSocket.isBound()) {
                 return;
             }
             try {
                 Thread.sleep(100);
             } catch (InterruptedException e) {
                 e.printStackTrace();
             }
         }
    }


    BlockingQueue requestQueue = new ArrayBlockingQueue(10);
    BlockingQueue responseQueue = new ArrayBlockingQueue(10);

    private class Listner extends Thread {

        public void start() {
            listen();
        }
    }
    private ServerSocket serverSocket = null;
    public void listen() {
        try {
            serverSocket = new ServerSocket(9000);
            Socket accept = serverSocket.accept();
            InputStream inputStream = accept.getInputStream();
            int length = inputStream.read();
            byte[] bytes = inputStream.readNBytes(length);

            responseQueue.add(bytes);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
