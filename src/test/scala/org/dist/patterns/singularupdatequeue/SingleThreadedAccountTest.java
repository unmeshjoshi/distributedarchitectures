package org.dist.patterns.singularupdatequeue;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SingleThreadedAccountTest {
    @Test
    public void testSingleThreadedAccountHandling() throws ExecutionException, InterruptedException {
        SingleThreadedAccount account = new SingleThreadedAccount(100);
        List<CompletableFuture<Response>> futures = new ArrayList();
        long start = System.nanoTime();
        for (int i = 0; i < 50000000; i++) {
            CompletableFuture<Response> future = i % 2 == 0 ? account.credit(1) : account.debit(1);
            futures.add(future);
        }
        for (CompletableFuture<Response> future : futures) {
            Response result = null;
            result = future.get();
        }
        long end = System.nanoTime();
        System.out.println("totalTime = " + TimeUnit.NANOSECONDS.toMillis(end - start));
    }

}