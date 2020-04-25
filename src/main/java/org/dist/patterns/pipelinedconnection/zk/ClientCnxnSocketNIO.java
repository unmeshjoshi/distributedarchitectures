package org.dist.patterns.pipelinedconnection.zk;

import org.dist.patterns.common.JsonSerDes;
import org.dist.patterns.common.RequestOrResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

public class ClientCnxnSocketNIO extends ClientCnxnSocket {
    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxnSocketNIO.class);
    static int sizeOfInt = 4;
    private final Selector selector = Selector.open();

    private SelectionKey sockKey;

    private SocketAddress localSocketAddress;

    private SocketAddress remoteSocketAddress;

    ClientCnxnSocketNIO() throws IOException {
    }

    @Override
    public boolean isConnected() {
        return sockKey != null;
    }

    ByteBuffer bb = null;

    void doIO(Queue<RequestOrResponse> pendingQueue, org.dist.patterns.pipelinedconnection.zk.ClientCnxn cnxn) throws InterruptedException, IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel();
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        if (sockKey.isReadable()) {
            int rc = sock.read(incomingBuffer);
            if (rc < 0) {
                throw new RuntimeException("Unable to read additional data from server sessionid 0x"
                        + Long.toHexString(sessionId)
                        + ", likely server has closed socket");
            }
            if (!incomingBuffer.hasRemaining()) {
                incomingBuffer.flip();
                if (incomingBuffer == lenBuffer) {
                    recvCount.getAndIncrement();
                    readLength();
                } else if (!initialized) {
                    readConnectResult();
                    enableRead();
                    if (findSendablePacket(outgoingQueue, false) != null) {
                        // Since SASL authentication has completed (if client is configured to do so),
                        // outgoing packets waiting in the outgoingQueue can now be sent.
                        enableWrite();
                    }
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                    initialized = true;
                } else {
                    sendThread.readResponse(incomingBuffer);
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                }
            }
        }
        if (sockKey.isWritable()) {
            RequestOrResponse response = findSendablePacket(outgoingQueue, false);
            if (response != null) {
                updateLastSend();
                // If we already started writing p, p.bb will already exist
                if (bb == null) {
                    String serialize = JsonSerDes.serialize(response);
                    byte[] bytes = serialize.getBytes();
                    bb = ByteBuffer.allocate(sizeOfInt + bytes.length);
                    bb.putInt(bytes.length);
                    bb.put(bytes);
                }
                sock.write(bb);
                if (!bb.hasRemaining()) {
                    sentCount.getAndIncrement();
                    outgoingQueue.removeFirstOccurrence(response);
                    synchronized (pendingQueue) {
                        pendingQueue.add(response);
                    }
                    bb = null;
                }

            }
        }
    }


    synchronized void enableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_WRITE);
        }
    }

    private synchronized void disableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) != 0) {
            sockKey.interestOps(i & (~SelectionKey.OP_WRITE));
        }
    }

    private synchronized void enableRead() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_READ) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_READ);
        }
    }

    @Override
    public void packetAdded() {
        wakeupCnxn();
    }

    @Override
    public void onClosing() {
        wakeupCnxn();
    }

    private synchronized void wakeupCnxn() {
        selector.wakeup();
    }


    SocketChannel createSock() throws IOException {
        SocketChannel sock;
        sock = SocketChannel.open();
        sock.configureBlocking(false);
        sock.socket().setSoLinger(false, -1);
        sock.socket().setTcpNoDelay(true);
        return sock;
    }

    void registerAndConnect(SocketChannel sock, InetSocketAddress addr) throws IOException {
        sockKey = sock.register(selector, SelectionKey.OP_CONNECT);
        boolean immediateConnect = sock.connect(addr);
        if (immediateConnect) {
            sendThread.primeConnection();
        }
    }

    @Override
    public void connect(InetSocketAddress addr) throws IOException {
        SocketChannel sock = createSock();
        try {
            registerAndConnect(sock, addr);
        } catch (IOException e) {
            LOG.error("Unable to open socket to " + addr);
            sock.close();
            throw e;
        }
        initialized = false;

        /*
         * Reset incomingBuffer
         */
        lenBuffer.clear();
        incomingBuffer = lenBuffer;
    }

    @Override
    public Object getLocalSocketAddress() {
        return localSocketAddress;
    }

    @Override
    public Object getRemoteSocketAddress() {
        return remoteSocketAddress;
    }

    @Override
    public void doTransport(int waitTimeOut, Queue<RequestOrResponse> pendingQueue, org.dist.patterns.pipelinedconnection.zk.ClientCnxn cnxn) throws IOException, InterruptedException {
        selector.select(waitTimeOut);
        Set<SelectionKey> selected;
        synchronized (this) {
            selected = selector.selectedKeys();
        }
        // Everything below and until we get back to the select is
        // non blocking, so time is effectively a constant. That is
        // Why we just have to do this once, here
        updateNow();
        for (SelectionKey k : selected) {
            SocketChannel sc = ((SocketChannel) k.channel());
            if ((k.readyOps() & SelectionKey.OP_CONNECT) != 0) {
                if (sc.finishConnect()) {
                    updateLastSendAndHeard();
                    updateSocketAddresses();
                    sendThread.primeConnection();
                }
            } else if ((k.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) {
                doIO(pendingQueue, cnxn);
            }
        }
        if (sendThread.getZkState().isConnected()) {
            if (findSendablePacket(outgoingQueue, false) != null) {
                enableWrite();
            }
        }
        selected.clear();
    }

    private void updateSocketAddresses() {
        Socket socket = ((SocketChannel) sockKey.channel()).socket();
        localSocketAddress = socket.getLocalSocketAddress();
        remoteSocketAddress = socket.getRemoteSocketAddress();
    }

    private RequestOrResponse findSendablePacket(LinkedBlockingDeque<RequestOrResponse> outgoingQueue, boolean tunneledAuthInProgres) {
        if (outgoingQueue.isEmpty()) {
            return null;
        }
        // If we've already starting sending the first packet, we better finish
        if (outgoingQueue.getFirst() != null) {
            return outgoingQueue.getFirst();
        }
        // Since client's authentication with server is in progress,
        // send only the null-header packet queued by primeConnection().
        // This packet must be sent so that the SASL authentication process
        // can proceed, but all other packets should wait until
        // SASL authentication completes.
        Iterator<RequestOrResponse> iter = outgoingQueue.iterator();
        while (iter.hasNext()) {
            RequestOrResponse p = iter.next();
//            if (p.requestHeader == null) {
//                // We've found the priming-packet. Move it to the beginning of the queue.
//                iter.remove();
//                outgoingQueue.addFirst(p);
//                return p;
//            } else {
//                // Non-priming packet: defer it until later, leaving it in the queue
//                // until authentication completes.
//                debug("deferring non-priming packet {} until SASL authentation completes.", p);
//            }
        }
        return null;
    }

}
