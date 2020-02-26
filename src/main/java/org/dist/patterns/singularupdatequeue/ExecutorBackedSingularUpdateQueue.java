package org.dist.patterns.singularupdatequeue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ExecutorBackedSingularUpdateQueue<P, R> {
    private UpdateHandler<P, R> updateHandler;
    private SingularUpdateQueue<R, ?> next;
    private ExecutorService singleThreadedExecutor = Executors.newSingleThreadExecutor();

    public ExecutorBackedSingularUpdateQueue(UpdateHandler<P, R> updateHandler) {
        this.updateHandler = updateHandler;
    }

    public ExecutorBackedSingularUpdateQueue(UpdateHandler<P, R> updateHandler, SingularUpdateQueue<R, ?> next) {
        this.updateHandler = updateHandler;
        this.next = next;
    }

    public void start() {
        //
    }

    public void submit(P request) {
        var completableFuture = new CompletableFuture<R>();

        Future<?> result = singleThreadedExecutor.submit(() -> {
            R update = updateHandler.update(request);
            completableFuture.complete(update);
        });

        if (next != null) {
           completableFuture.thenAccept(s -> next.submit(s));
        }
    }
}
