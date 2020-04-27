package org.dist.patterns.singularupdatequeue;

import java.util.concurrent.CompletableFuture;

public class SingleThreadedAccount {
    private SingularUpdateQueue<Request, Response> queue;
    private int balance = 0;

    public SingleThreadedAccount(int balance) {
        this.balance = balance;
        this.queue   = new SingularUpdateQueue<Request, Response>(this::handleMessage);
        this.queue.start();
    }


    private Response handleMessage(Request request) {
        if (request.requestType == RequestType.CREDIT) {
           balance += request.amount;
           return new Response().withAmount(balance);

        } else if (request.requestType == RequestType.DEBIT) {

           balance -= request.amount;
           return new Response().withAmount(balance);
        }
        throw new IllegalArgumentException("Unknown request type " + request.requestType);
    };


    public CompletableFuture<Response> credit(int amount) {
        return queue.submit(new Request(amount, RequestType.CREDIT));
    }

    public CompletableFuture<Response> debit(int amount) {
        return queue.submit(new Request(amount, RequestType.DEBIT));
    }
}
