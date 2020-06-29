package org.dist.patterns.singularupdatequeue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.dist.bookkeeper.Journal;
import org.dist.kvstore.InetAddressAndPort;
import org.dist.patterns.wal.WriteAheadLog;

import java.io.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

public class SingleThreadedAccount {
    private final Journal journal;
    private SingularUpdateQueue<Request, Response> queue;
    private int balance = 0;
    private WriteAheadLog log;

    public SingleThreadedAccount(int balance, File dir) {
        this.balance = balance;
        this.log = WriteAheadLog.openWAL(0, dir);
        journal = new Journal(dir);
        journal.start();
        this.queue   = new SingularUpdateQueue<Request, Response>(this::handleMessage);
        this.queue.start();
    }
    int entryId = 1;

    private CompletableFuture<Response> addToJournal(int balance) {
        CompletableFuture<Response> future = new CompletableFuture<Response>();
        Journal.WriteCallback writeCallback = (int rc, long ledgerId, long entryId1, InetAddressAndPort addr, Object ctx) -> {
            future.complete(new Response().withAmount(entryId));
        };
        int ledgerId = 1;
        try {
            journal.logAddEntry(ledgerId, entryId++, createByteBuf(ledgerId, balance, 16), false, writeCallback, this);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return future;
    }


    private static ByteBuf createByteBuf(long ledgerId, long balance, int entrySize) {
        byte[] data = new byte[entrySize];
        ThreadLocalRandom.current().nextBytes(data);
        ByteBuf buffer = Unpooled.wrappedBuffer(data);
        buffer.writerIndex(0);
        buffer.writeLong(ledgerId);
        buffer.writeLong(balance);
        buffer.writerIndex(entrySize);
        return buffer;
    }

    private void writeToFile(int balance) {
        log.write(balance + "");
    }

    private Response handleMessage(Request request) {
        if (request.requestType == RequestType.CREDIT) {
           balance += request.amount;
           writeToFile(balance);
           return new Response().withAmount(balance);

        } else if (request.requestType == RequestType.DEBIT) {
           balance -= request.amount;
            writeToFile(balance);
            return new Response().withAmount(balance);
        }
        throw new IllegalArgumentException("Unknown request type " + request.requestType);
    };


    public CompletableFuture<Response> credit(int amount) {
//        return addToJournal(amount);
        return queue.submit(new Request(amount, RequestType.CREDIT));
    }

    public CompletableFuture<Response> debit(int amount) {
//        return addToJournal(amount);
        return queue.submit(new Request(amount, RequestType.DEBIT));
    }
}
