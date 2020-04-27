package org.dist.patterns.singularupdatequeue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class SynchronizedAccount {
    private int balance;
    private File file;
    private FileOutputStream fileOutputStream;

    public SynchronizedAccount(int balance, File dir) {
        this.balance = balance;
        try {
            this.file = new File(dir.getAbsolutePath() + "/test");
            this.file.createNewFile();
            fileOutputStream = new FileOutputStream(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        try {
            fileOutputStream.write(balance);
            fileOutputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
