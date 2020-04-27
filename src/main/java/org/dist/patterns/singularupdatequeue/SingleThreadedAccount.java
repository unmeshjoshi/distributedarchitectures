package org.dist.patterns.singularupdatequeue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class SingleThreadedAccount {
    private SingularUpdateQueue<Request, Response> queue;
    private int balance = 0;
    private File file;
    private FileOutputStream fileOutputStream;

    public SingleThreadedAccount(int balance, File dir) {
        this.balance = balance;
        try {
            this.file = new File(dir.getAbsolutePath() + "/test");
            this.file.createNewFile();
            fileOutputStream = new FileOutputStream(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.queue   = new SingularUpdateQueue<Request, Response>(this::handleMessage);
        this.queue.start();
    }

    private void writeToFile(int balance) {
        try {
            fileOutputStream.write(balance);
            fileOutputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Response handleMessage(Request request) {
        if (request.requestType == RequestType.CREDIT) {
           balance += request.amount;
           return new Response().withAmount(balance);

        } else if (request.requestType == RequestType.DEBIT) {

           balance -= request.amount;
           return new Response().withAmount(balance);
        }
        writeToFile(balance);
        throw new IllegalArgumentException("Unknown request type " + request.requestType);
    };


    public CompletableFuture<Response> credit(int amount) {
        return queue.submit(new Request(amount, RequestType.CREDIT));
    }

    public CompletableFuture<Response> debit(int amount) {
        return queue.submit(new Request(amount, RequestType.DEBIT));
    }
}
