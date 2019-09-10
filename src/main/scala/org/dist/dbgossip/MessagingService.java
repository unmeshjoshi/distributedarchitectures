package org.dist.dbgossip;

public class MessagingService {
    public static MessagingService instance = new MessagingService();
    public static MessagingService instance() {
        return instance;
    }

    public void send(Message message, InetAddressAndPort to) {

    }

    public void waitUntilListening() {

    }
}
