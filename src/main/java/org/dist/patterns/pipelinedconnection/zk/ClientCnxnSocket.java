package org.dist.patterns.pipelinedconnection.zk;

import org.apache.zookeeper.common.Time;
import org.dist.patterns.common.RequestOrResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ClientCnxnSocket {
    protected final AtomicLong sentCount = new AtomicLong(0L);
    protected final AtomicLong recvCount = new AtomicLong(0L);
    protected long sessionId;

    protected boolean initialized;

    protected final ByteBuffer lenBuffer = ByteBuffer.allocateDirect(4);
    /**
     * After the length is read, a new incomingBuffer is allocated in
     * readLength() to receive the full message.
     */
    protected ByteBuffer incomingBuffer = lenBuffer;
    protected LinkedBlockingDeque<RequestOrResponse> outgoingQueue;
    protected ClientCnxn.SendThread sendThread;
    protected long lastHeard;
    protected long lastSend;
    protected long now;

    void introduce(ClientCnxn.SendThread sendThread, long sessionId, LinkedBlockingDeque<RequestOrResponse> outgoingQueue) {
        this.sendThread = sendThread;
        this.sessionId = sessionId;
        this.outgoingQueue = outgoingQueue;
    }

    void updateNow() {
        now = System.nanoTime() / 1000000;
    }

    void readLength() throws IOException {
        int len = incomingBuffer.getInt();
        if (len < 0) {
            throw new IOException("Length " + len + " is out of range!");
        }
        incomingBuffer = ByteBuffer.allocate(len);
    }

    void updateLastSend() {
        this.lastSend = now;
    }

    void readConnectResult() throws IOException {
        this.sessionId =  -1;////conRsp.getSessionId();
        sendThread.onConnected();
    }

    void updateLastHeard() {
        this.lastHeard = now;
    }

    public abstract boolean isConnected();

    public abstract void packetAdded();

    public abstract void onClosing();

    void updateLastSendAndHeard() {
        this.lastSend = now;
        this.lastHeard = now;
    }

    public abstract void connect(InetSocketAddress addr) throws IOException;

    public abstract Object getLocalSocketAddress();

    public abstract Object getRemoteSocketAddress();

    public int getIdleRecv() {
        return (int) (now - lastHeard);
    }

    public abstract void doTransport(int to, Queue<RequestOrResponse> pendingQueue,  org.dist.patterns.pipelinedconnection.zk.ClientCnxn cnxn) throws IOException, InterruptedException;
}
