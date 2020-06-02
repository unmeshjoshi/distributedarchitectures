package org.dist.patterns.singularupdatequeue;

import org.dist.kvstore.JsonSerDes;
import org.dist.patterns.replicatedlog.AppendEntriesRequest;

import java.util.concurrent.Callable;

class SynchronizedAccountCallable implements Callable<SynchronizedAccountCallable.SynchronizedAccountPerfFuture> {
    private static long runTimeInMillis = SingleThreadedAccountPerfMain.TEST_TIME;
    private SynchronizedAccount account;

    public SynchronizedAccountCallable(SynchronizedAccount account) {
        this.account = account;
    }

    @Override
    public SynchronizedAccountPerfFuture call() throws Exception {
        long iterations = 0L, elapsedTime = 0L;
        long startTime = System.currentTimeMillis();
        double iterationsPerSecond = 0;
        do {
            iterations++;
            // Just in case there 'iterations' is about to overflow
            if (iterations == Long.MAX_VALUE) {
                long elapsed = System.currentTimeMillis() - startTime;
                iterationsPerSecond =
                        iterations / ((double) (elapsed / 1000));
                System.err.println(
                        "Iteration counter about to overflow ...");
                System.err.println(
                        "Calculating current operations per second ...");
                System.err.println(
                        "Iterations per second: " + iterationsPerSecond);
                iterations = 0L;
                startTime = System.currentTimeMillis();
                runTimeInMillis -= elapsed;
            }

            String ser = JsonSerDes.serialize(new AppendEntriesRequest(1200, new byte[0], 1l));

            if (iterations % 1001 == 0) {
                account.credit(100);
            } else if (iterations % 60195 == 0) {
                account.debit(1);
            } else {
                account.credit(10);
            }

            AppendEntriesRequest deser = JsonSerDes.deserialize(ser, AppendEntriesRequest.class);

            if (iterations % 1000 == 0) {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        } while (elapsedTime < runTimeInMillis);

        if (iterations >= 1000) {
            iterationsPerSecond =
                    iterations / ((double) (elapsedTime / 1000));
        }
        SynchronizedAccountPerfFuture accountPerfFuture =
                new SynchronizedAccountPerfFuture(iterationsPerSecond
                );
        return accountPerfFuture;
    }

    public class SynchronizedAccountPerfFuture {
        private double iterationsPerSecond;
        public SynchronizedAccountPerfFuture(double iterationsPerSecond) {
            this.iterationsPerSecond = iterationsPerSecond;
        }

        public double getIterationsPerSecond() {
            return iterationsPerSecond;
        }
    }
}

