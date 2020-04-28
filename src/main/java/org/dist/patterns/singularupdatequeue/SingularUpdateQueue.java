package org.dist.patterns.singularupdatequeue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class SingularUpdateQueue<Request, Response> extends Thread {
    private LinkedBlockingDeque<RequestWrapper<Request, Response>> workQueue = new LinkedBlockingDeque<RequestWrapper<Request, Response>>();
    private UpdateHandler<Request, Response> updateHandler;
    private SingularUpdateQueue<Response, ?> next;

    public SingularUpdateQueue(UpdateHandler<Request, Response> updateHandler) {
        super("SingularUpdateQueue");
        this.updateHandler = updateHandler;
    }

    static class RequestWrapper<P, R> {
        private final CompletableFuture<R> future;
        private final P request;

        public RequestWrapper(P request) {
            this.request = request;
            this.future = new CompletableFuture<R>();
        }

        public CompletableFuture<R> getFuture() {
            return this.future;
        }

        public P getRequest() {
            return request;
        }

        public void complete(R response) {
            future.complete(response);
        }
    }

    public CompletableFuture<Response> submit(Request request) {
        try {
            RequestWrapper<Request, Response> wrapper = new RequestWrapper<>(request);
            workQueue.put(wrapper);
            return wrapper.future;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        try {
            while(true) {
                RequestWrapper<Request, Response> wrapper = workQueue.take();
                Request request = (Request) wrapper.request;
                Response response = updateHandler.update(request);
                if (next != null) {
                    next.submit(response);
                }
                wrapper.future.complete(response);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
