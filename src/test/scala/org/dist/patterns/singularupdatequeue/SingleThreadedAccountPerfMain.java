package org.dist.patterns.singularupdatequeue;

import org.dist.utils.JTestUtils;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class SingleThreadedAccountPerfMain {

    final public static int TEST_TIME = 30 * 1000;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final int numberOfThreads =
                Runtime.getRuntime().availableProcessors();

        ExecutorService pool =
                Executors.newFixedThreadPool(numberOfThreads);
        Callable<SingleThreadedAccountCallable.AccountPerfFuture>[] callables =
                new SingleThreadedAccountCallable[numberOfThreads];
        SingleThreadedAccount account = new SingleThreadedAccount(100, JTestUtils.tmpDir("perf"));
        for (int i = 0; i < callables.length; i++) {
            callables[i] = new SingleThreadedAccountCallable(account);
        }
        System.out.println("\tthreads allocated.");

        Set<Future<SingleThreadedAccountCallable.AccountPerfFuture>> set =
                new HashSet<Future<SingleThreadedAccountCallable.AccountPerfFuture>>();
        for (int i = 0; i < callables.length; i++) {
            Callable<SingleThreadedAccountCallable.AccountPerfFuture> callable = callables[i];
            Future<SingleThreadedAccountCallable.AccountPerfFuture> future = pool.submit(callable);
            set.add(future);
        }

        System.out.println("\t(" + callables.length +
                ") threads started.");
// block and wait for all Callables to finish their
        System.out.println("Waiting for " + TEST_TIME / 1000 +
                " seconds for (" + callables.length +
                ") threads to complete ...");
        double iterationsPerSecond = 0;
        int counter = 1;
        long totalExecutedCommands = 0;
        for (Future<SingleThreadedAccountCallable.AccountPerfFuture> future : set) {
            SingleThreadedAccountCallable.AccountPerfFuture result = null;
            try {
                result = future.get();
            } catch (Exception ex) {
              throw new RuntimeException(ex);
            }
            List<CompletableFuture<Response>> responseFutures = result.getResponseFutures();
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]));

            allFutures.get();

            totalExecutedCommands += responseFutures.size();
            System.out.println("Iterations per second on thread[" +
                    counter++ + "] -> " +
                    result.getIterationsPerSecond());
            iterationsPerSecond += result.getIterationsPerSecond();
        }
// print number of totals
        DecimalFormat df = new DecimalFormat("#.##");
        System.out.println("Total executed commands -> " +
                df.format(totalExecutedCommands));


        System.out.println("Total iterations per second -> " +
                df.format(iterationsPerSecond));
        NumberFormat nf = NumberFormat.getInstance();
        System.exit(0);

    }

}
