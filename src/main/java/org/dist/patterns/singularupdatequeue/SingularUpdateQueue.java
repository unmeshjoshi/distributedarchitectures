package org.dist.patterns.singularupdatequeue;

import java.util.concurrent.ArrayBlockingQueue;

public class SingularUpdateQueue<P, R> extends Thread {
    private ArrayBlockingQueue<P> workQueue = new ArrayBlockingQueue<P>(100);
    private UpdateHandler<P, R> updateHandler;
    private SingularUpdateQueue<R, ?> next;

    public SingularUpdateQueue(UpdateHandler<P, R> updateHandler) {
        this.updateHandler = updateHandler;
    }

    public SingularUpdateQueue(UpdateHandler<P, R> updateHandler, SingularUpdateQueue<R, ?> next) {
        this.updateHandler = updateHandler;
        this.next = next;
    }

    public void submit(P request) {
        workQueue.add(request);
    }

    @Override
    public void run() {
        try {
            P request = workQueue.take();
            R response = updateHandler.update(request);
            if (next != null) {
                next.submit(response);
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
