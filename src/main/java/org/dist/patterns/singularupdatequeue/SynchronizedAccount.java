package org.dist.patterns.singularupdatequeue;

import org.dist.patterns.wal.WriteAheadLog;

import java.io.*;

public class SynchronizedAccount {
    private int balance;
    private WriteAheadLog log;
    public SynchronizedAccount(int balance, File dir) {
        this.balance = balance;
        this.log = WriteAheadLog.openWAL(0, dir);
    }

    public synchronized int credit(int amount) {
        balance += amount;
        writeToFile(balance);
        return balance;
    }

    private long t = System.nanoTime();
    private volatile long consumeCPU = 0;

    public synchronized int debit(int amount) {
        balance -= amount;
        writeToFile(balance);
        return balance;
    }

    private void writeToFile(int balance) {
       log.write(balance + "");
    }
}
