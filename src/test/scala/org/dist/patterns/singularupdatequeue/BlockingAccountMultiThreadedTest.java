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

import static org.junit.Assert.*;

public class BlockingAccountMultiThreadedTest {

    @Test
    public void testMultiThreadedAccountHandling() throws ExecutionException, InterruptedException {
        BlockingAccount account = new BlockingAccount(100);
        int noOfCores = Runtime.getRuntime().availableProcessors() * 10;
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(noOfCores, noOfCores, 1000, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
        List<Future<?>> futures = new ArrayList();
        long start = System.nanoTime();
        for (int i = 0; i < 50000000; i++) {
            if(i % 2 == 0) {
                Future<?> submit = threadPoolExecutor.submit(() -> account.credit(1));
                futures.add(submit);
            } else {
                Future<?> submit = threadPoolExecutor.submit(() -> account.debit(1));
                futures.add(submit);
            }

        }

        for (Future<?> future : futures) {
            future.get();
        }
        
        long end = System.nanoTime();
        System.out.println("totalTime = " + TimeUnit.NANOSECONDS.toMillis(end - start));
    }

}