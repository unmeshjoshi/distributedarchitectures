package org.dist.patterns.pipelinedconnection.zk;

import org.apache.zookeeper.ZooKeeper;
import org.dist.patterns.common.JsonSerDes;
import org.dist.patterns.common.RequestOrResponse;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class ClientCnxn {
    /**
     * These are the packets that have been sent and are waiting for a response.
     */
    private final Queue<RequestOrResponse> pendingQueue = new ArrayDeque<>();

    /**
     * These are the packets that need to be sent.
     */
    private final LinkedBlockingDeque<RequestOrResponse> outgoingQueue = new LinkedBlockingDeque<RequestOrResponse>();

    private int connectTimeout;
    private int readTimeout;
    private EventThread eventThread;
    Consumer<RequestOrResponse> consumer;
    private SendThread sendThread;
    private long sessionId;
    private volatile boolean closing = false;
    private ZooKeeper.States state;


    class EventThread extends Thread {
        private final LinkedBlockingQueue<Object> waitingEvents = new LinkedBlockingQueue<Object>();
        public void queuePacket(RequestOrResponse response) {
            waitingEvents.add(response);
        }
        @Override
        public void run() {

        }
    }

    public void queuePacket(RequestOrResponse request) {
        outgoingQueue.add(request);
        sendThread.getClientCnxnSocket().packetAdded();
    }

    class SendThread extends Thread {

        private long lastPingSentNs;
        private final ClientCnxnSocket clientCnxnSocket;
        private Random r = new Random();
        private boolean isFirstConnect = true;
        private boolean isRunning;
        private InetSocketAddress rwServerAddress = null;

        SendThread(ClientCnxnSocket clientCnxnSocket) {
            this.clientCnxnSocket = clientCnxnSocket;
        }

        public void onConnected() {

        }

        private void startConnect(InetSocketAddress addr) throws IOException {
            state = ZooKeeper.States.CONNECTING;

            String hostPort = addr.getHostString() + ":" + addr.getPort();
            logStartConnect(addr);

            clientCnxnSocket.connect(addr);
        }

        private void logStartConnect(InetSocketAddress addr) {
            String msg = "Opening socket connection to server " + addr;

            LOG.info(msg);
        }
        @Override
        public void run() {
            isRunning = true;
            int to = 0;
            while(isRunning) {
                try {
                    clientCnxnSocket.introduce(this, sessionId, outgoingQueue);
                    clientCnxnSocket.updateNow();
                    clientCnxnSocket.updateLastSendAndHeard();
                    InetSocketAddress serverAddress = null;
                    if (!clientCnxnSocket.isConnected()) {
                        // don't re-establish connection if we are closing
                        if (closing) {
                            break;
                        }

                        if (rwServerAddress != null) {
                            serverAddress = rwServerAddress;
                            rwServerAddress = null;
                        }
//                    } else {
//                        serverAddress = hostProvider.next(1000);
//                    }
                        startConnect(serverAddress);
                        clientCnxnSocket.updateLastSendAndHeard();
                    }

                    if (state.isConnected()) {
                        to = connectTimeout - clientCnxnSocket.getIdleRecv();
                    }

                    if (to <= 0) {
                        String warnInfo;
                        warnInfo = "Client session timed out, have not heard from server in "
                                + clientCnxnSocket.getIdleRecv() + "ms"
                                + " for sessionid 0x" + Long.toHexString(sessionId);
                        LOG.warn(warnInfo);
                        throw new RuntimeException(warnInfo);
                    }

                    clientCnxnSocket.doTransport(to, pendingQueue, ClientCnxn.this);


                } catch (Exception e) {
                    LOG.error(e.getMessage());
                }
            }
        }

        void readResponse(ByteBuffer incomingBuffer) throws IOException {
            byte[] array = incomingBuffer.array();
            RequestOrResponse response = JsonSerDes.deserialize(array, RequestOrResponse.class);
//
//            if (replyHdr.getXid() == -2) {
//                // -2 is the xid for pings
//                if (LOG.isDebugEnabled()) {
//                    LOG.debug("Got ping response for sessionid: 0x"
//                            + Long.toHexString(sessionId)
//                            + " after "
//                            + ((System.nanoTime() - lastPingSentNs) / 1000000)
//                            + "ms");
//                }
//                return;
//            }

            RequestOrResponse request;

            synchronized (pendingQueue) {
                if (pendingQueue.size() == 0) {
                    throw new IOException("Nothing in the queue, but got " + response.getCorrelationId());
                }
                request = pendingQueue.remove();
            }

            try {
                if (request.getCorrelationId() != response.getCorrelationId()) {

                    throw new IOException("Xid out of order. Got Xid " + response.getCorrelationId()
                            + " expected Xid " + request.getCorrelationId()
                            + " for a packet with details: " + response);
                }
            } finally {
                finishPacket(response);
            }
        }

        public ClientCnxnSocket getClientCnxnSocket() {
            return clientCnxnSocket;
        }

        public void primeConnection() {
            LOG.info(
                    "Socket connection established, initiating session client: " +
                    clientCnxnSocket.getLocalSocketAddress() + " server:" +
                    clientCnxnSocket.getRemoteSocketAddress());
        }

        public ZooKeeper.States getZkState() {
            return state;
        }
    }

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ClientCnxn.class.getName());

    private void finishPacket(RequestOrResponse response) {
        eventThread.queuePacket(response);
    }
}
