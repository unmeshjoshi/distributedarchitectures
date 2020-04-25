package org.dist.patterns.singularupdatequeue;

import java.util.concurrent.CompletableFuture;

public class SingleThreadedAccount {
    int balance = 0;

    public SingleThreadedAccount(int balance) {
        this.balance = balance;
    }

    SingularUpdateQueue<Request, Response> queue
            = new SingularUpdateQueue<Request, Response>((request) -> {
        if (request.requestType == RequestType.CREDIT) {

           balance += request.amount;
           return new Response().withAmount(balance);
        } else if (request.requestType == RequestType.DEBIT) {

           balance -= request.amount;
           return new Response().withAmount(balance);
        }
        return Response.None;
    });
    {
        queue.start();
    }

    public CompletableFuture<Response> credit(int amount) {
        return queue.submit(new Request(amount, RequestType.CREDIT));
    }

    public CompletableFuture<Response> debit(int amount) {
        return queue.submit(new Request(amount, RequestType.DEBIT));
    }
}
