package org.dist.patterns.singularupdatequeue;

public class BlockingAccount {
    private int balance;

    public BlockingAccount(int balance) {
        this.balance = balance;
    }

    public synchronized void credit(int amount) {
        balance += amount;
    }

    public synchronized void debit(int amount) {
        balance -= amount;
    }
}
