package org.dist.patterns.singularupdatequeue;

import java.util.concurrent.Callable;

class SynchronizedAccountCallable implements Callable<SynchronizedAccountCallable.SynchronizedAccountPerfFuture> {
    private static long runTimeInMillis = SingleThreadedAccountPerfMain.TEST_TIME;
    private long nullCounter, recordsRemoved, newRecordsAdded;
    private int index;
    private String taxPayerId;
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

            if (iterations % 1001 == 0) {
                account.credit(100);
            } else if (iterations % 60195 == 0) {
                account.debit(1);
            } else {
                account.credit(10);
            }

            if (iterations % 1000 == 0) {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        } while (elapsedTime < runTimeInMillis);

        if (iterations >= 1000) {
            iterationsPerSecond =
                    iterations / ((double) (elapsedTime / 1000));
        }
        SynchronizedAccountPerfFuture accountPerfFuture =
                new SynchronizedAccountPerfFuture(iterationsPerSecond, newRecordsAdded,
                        recordsRemoved, nullCounter);
        return accountPerfFuture;
    }

    public class SynchronizedAccountPerfFuture {
        private double iterationsPerSecond;
        private long recordsAdded, recordsRemoved, nullCounter;
        public SynchronizedAccountPerfFuture(double iterationsPerSecond, long recordsAdded,
                                 long recordsRemoved, long nullCounter) {
            this.iterationsPerSecond = iterationsPerSecond;
            this.recordsAdded = recordsAdded;
            this.recordsRemoved = recordsRemoved;
            this.nullCounter = nullCounter;
        }

        public double getIterationsPerSecond() {
            return iterationsPerSecond;
        }
        public long getRecordsAdded() {
            return recordsAdded;
        }
        public long getRecordsRemoved() {
            return recordsRemoved;
        }
        public long getNullCounter() {
            return nullCounter;
        }
    }
}

