package org.dist.patterns.singularupdatequeue;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SingleThreadedAccountTest {
    @Test
    public void testSingleThreadedAccountHandling() throws ExecutionException, InterruptedException {
        SingleThreadedAccount account = new SingleThreadedAccount(100);
        int noOfCores = Runtime.getRuntime().availableProcessors();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(noOfCores, noOfCores, 1000, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
        List<Future<?>> futures = new ArrayList();
        long start = System.nanoTime();
        for (int i = 0; i < 50000000; i++) {
            Runnable task = i % 2 == 0 ? ()->account.credit(1) : ()->account.debit(1);
            Future<?> future = threadPoolExecutor.submit(task);
            futures.add(future);
        }
        for (Future<?> future : futures) {
            future.get();
        }
        long end = System.nanoTime();
        System.out.println("totalTime = " + TimeUnit.NANOSECONDS.toMillis(end - start));
    }

}