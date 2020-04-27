package org.dist.patterns.singularupdatequeue;

import org.dist.queue.TestUtils;
import org.dist.utils.JTestUtils;

import java.io.File;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class SynchronizedAccountPerfMain {

    final public static int TEST_TIME = 60 * 500;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final int numberOfThreads =
                Runtime.getRuntime().availableProcessors();

        List<String>[] taxPayerList = new ArrayList[numberOfThreads];
        ExecutorService pool =
                Executors.newFixedThreadPool(numberOfThreads);
        Callable<SynchronizedAccountCallable.SynchronizedAccountPerfFuture>[] callables =
                new SynchronizedAccountCallable[numberOfThreads];
        SynchronizedAccount account = new SynchronizedAccount(100, JTestUtils.tmpDir("perf"));
        for (int i = 0; i < callables.length; i++) {
            callables[i] = new SynchronizedAccountCallable(account);
        }
        System.out.println("\tthreads allocated.");

        Set<Future<SynchronizedAccountCallable.SynchronizedAccountPerfFuture>> set =
                new HashSet<>();
        for (int i = 0; i < callables.length; i++) {
            Callable<SynchronizedAccountCallable.SynchronizedAccountPerfFuture> callable = callables[i];
            Future<SynchronizedAccountCallable.SynchronizedAccountPerfFuture> future = pool.submit(callable);
            set.add(future);
        }

        System.out.println("\t(" + callables.length +
                ") threads started.");
// block and wait for all Callables to finish their
        System.out.println("Waiting for " + TEST_TIME / 1000 +
                " seconds for (" + callables.length +
                ") threads to complete ...");
        double iterationsPerSecond = 0;
        long recordsAdded = 0, recordsRemoved = 0;
        long nullCounter = 0; int counter = 1;
        long totalExecutedCommands = 0;
        for (Future<SynchronizedAccountCallable.SynchronizedAccountPerfFuture> future : set) {
            SynchronizedAccountCallable.SynchronizedAccountPerfFuture result = null;
            try {
                result = future.get();
            } catch (Exception ex) {
              throw new RuntimeException(ex);
            }
            System.out.println("Iterations per second on thread[" +
                    counter++ + "] -> " +
                    result.getIterationsPerSecond());
            iterationsPerSecond += result.getIterationsPerSecond();
            recordsAdded += result.getRecordsAdded();
            recordsRemoved += result.getRecordsRemoved();
            nullCounter = result.getNullCounter();
        }
// print number of totals
        DecimalFormat df = new DecimalFormat("#.##");

        System.out.println("Total iterations per second -> " +
                df.format(iterationsPerSecond));
        NumberFormat nf = NumberFormat.getInstance();
        System.out.println("Total records added ---------> " +
                nf.format(recordsAdded));
        System.out.println("Total records removed -------> " +
                nf.format(recordsRemoved));
//        System.out.println("Total records in db ---------> " +
//                nf.format(db.size()));
        System.out.println("Total null records encountered: " +
                nf.format(nullCounter));
        System.exit(0);

    }

}
