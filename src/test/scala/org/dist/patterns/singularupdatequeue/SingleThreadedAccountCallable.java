package org.dist.patterns.singularupdatequeue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

class SingleThreadedAccountCallable implements Callable {
    private static long runTimeInMillis = SingleThreadedAccountPerfMain.TEST_TIME;
    private long nullCounter, recordsRemoved, newRecordsAdded;
    private int index;
    private String taxPayerId;
    private SingleThreadedAccount account;

    public SingleThreadedAccountCallable(SingleThreadedAccount account) {
        this.account = account;
    }

    @Override
    public AccountPerfFuture call() throws Exception {
        long iterations = 0L, elapsedTime = 0L;
        long startTime = System.currentTimeMillis();
        double iterationsPerSecond = 0;
        List<CompletableFuture<Response>> responseFutures = new ArrayList<>();
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
            CompletableFuture<Response> accountFuture;
            if (iterations % 1001 == 0) {
                accountFuture = account.credit(100);
            } else if (iterations % 60195 == 0) {
                accountFuture = account.debit(1);
            } else {
                accountFuture = account.credit(10);
            }
            responseFutures.add(accountFuture);
            if (iterations % 1000 == 0) {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        } while (elapsedTime < runTimeInMillis);

        if (iterations >= 1000) {
            iterationsPerSecond =
                    iterations / ((double) (elapsedTime / 1000));
        }
        AccountPerfFuture accountPerfFuture =
                new AccountPerfFuture(responseFutures, iterationsPerSecond, newRecordsAdded,
                        recordsRemoved, nullCounter);
        return accountPerfFuture;
    }

    public class AccountPerfFuture {
        private List<CompletableFuture<Response>> responseFutures;
        private double iterationsPerSecond;
        private long recordsAdded, recordsRemoved, nullCounter;
        public AccountPerfFuture(List<CompletableFuture<Response>> responseFutures, double iterationsPerSecond, long recordsAdded,
                                 long recordsRemoved, long nullCounter) {
            this.responseFutures = responseFutures;
            this.iterationsPerSecond = iterationsPerSecond;
            this.recordsAdded = recordsAdded;
            this.recordsRemoved = recordsRemoved;
            this.nullCounter = nullCounter;
        }

        public List<CompletableFuture<Response>> getResponseFutures() {
            return responseFutures;
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

