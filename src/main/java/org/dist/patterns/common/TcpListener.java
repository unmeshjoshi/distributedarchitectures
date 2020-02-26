package org.dist.patterns.common;

import org.dist.patterns.singularupdatequeue.ExecutorBackedSingularUpdateQueue;
import org.dist.patterns.singularupdatequeue.SingularUpdateQueue;
import org.dist.patterns.singularupdatequeue.UpdateHandler;
import org.dist.patterns.wal.WAL;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

public class TcpListener extends Thread {
    private InetAddressAndPort listenIp;
    private final ServerSocket serverSocket;

    public TcpListener(InetAddressAndPort listenIp) {
        this.listenIp = listenIp;
        try {
            this.serverSocket = new ServerSocket();
            this.serverSocket.bind(new InetSocketAddress(listenIp.getAddress(), listenIp.getPort()));
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }

    private AtomicBoolean running = new AtomicBoolean(true);

    private UpdateHandler<Pair<RequestOrResponse>, RequestOrResponse> handler = new UpdateHandler<Pair<RequestOrResponse>, RequestOrResponse>() {
        public RequestOrResponse update(Pair<RequestOrResponse> pair) {
            RequestOrResponse request = pair.requestOrResponse;
            pair.socket.write(request);
            return request;
        }
    };
    private SingularUpdateQueue socketWriterQueue = new SingularUpdateQueue(handler);


    private UpdateHandler<Pair<RequestOrResponse>, Pair<RequestOrResponse>> walHandler = new UpdateHandler<Pair<RequestOrResponse>, Pair<RequestOrResponse>>() {
        private WAL wal = new WAL();
        @Override
        public Pair<RequestOrResponse> update(Pair<RequestOrResponse> pair) {
            SocketIO<RequestOrResponse> socket = pair.socket;
            wal.write(pair.requestOrResponse.getMessageBodyJson());
            RequestOrResponse response = new RequestOrResponse(pair.requestOrResponse.getRequestId(), "", pair.requestOrResponse.getCorrelationId());
            return new Pair<RequestOrResponse>(response, socket);
        }
    };
    private ExecutorBackedSingularUpdateQueue walWriterQueue = walUpdateQueue(socketWriterQueue);

    private ExecutorBackedSingularUpdateQueue walUpdateQueue(SingularUpdateQueue socketWriterQueue) {
        return new ExecutorBackedSingularUpdateQueue(walHandler, socketWriterQueue);
    }

    static class Pair<T> {
        private final T requestOrResponse;
        private final SocketIO<T> socket;
        public Pair(T t, SocketIO<T> socket) {
            this.requestOrResponse = t;
            this.socket = socket;
        }
    }

    @Override
    public void run() {
        walWriterQueue.start();
        socketWriterQueue.start();

        while(running.get()) {
            try {
                var socket = this.serverSocket.accept();
                final var socketIo = new SocketIO<RequestOrResponse>(socket, RequestOrResponse.class);
                BiFunction<RequestOrResponse, Socket, RequestOrResponse> handler = (requestOrResponse, clientSocket) -> {
                    walWriterQueue.submit(new Pair<RequestOrResponse>(requestOrResponse, socketIo));
                    return requestOrResponse;
                };
                socketIo.readHandleWithSocket(handler);

            } catch (IOException e) {

            }
        }
    }

    public void shudown() {
        try(serverSocket) {
        } catch (IOException e) {
        }
        running.set(false);
    }

}
