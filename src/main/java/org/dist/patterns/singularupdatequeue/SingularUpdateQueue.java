package org.dist.patterns.singularupdatequeue;

import java.util.concurrent.ArrayBlockingQueue;

public class SingularUpdateQueue<T, V> extends Thread {
    private ArrayBlockingQueue<T> workQueue = new ArrayBlockingQueue<T>(100);
    private UpdateHandler<T, V> updateHandler;
    private SingularUpdateQueue<V, ?> next;

    public SingularUpdateQueue(UpdateHandler<T, V> updateHandler) {
        this.updateHandler = updateHandler;
    }

    public SingularUpdateQueue(UpdateHandler<T, V> updateHandler, SingularUpdateQueue<V, ?> next) {
        this.updateHandler = updateHandler;
        this.next = next;
    }

    public void submit(T request) {
        workQueue.add(request);
    }

    @Override
    public void run() {
        try {
            T request = workQueue.take();
            V response = updateHandler.update(request);
            if (next != null) {
                next.submit(response);
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
