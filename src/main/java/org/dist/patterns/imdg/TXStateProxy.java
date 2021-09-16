package org.dist.patterns.imdg;

public interface TXStateProxy extends TXStateInterface {
    boolean isInProgress();

    void lock();

    void release();

    void unlock();
}
