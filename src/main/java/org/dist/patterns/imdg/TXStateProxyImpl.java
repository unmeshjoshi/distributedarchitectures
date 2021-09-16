package org.dist.patterns.imdg;

import java.util.ArrayList;
import java.util.List;

public class TXStateProxyImpl implements TXStateProxy {
    TransactionId id;
    private List<EntryEventImpl> pendingValue = new ArrayList<>();

    public TXStateProxyImpl(TransactionId id) {
        this.id = id;
    }

    @Override
    public void putEntry(EntryEventImpl entryEvent) {
        pendingValue.add(entryEvent);
    }

    @Override
    public TransactionId getTransactionId() {
        return id;
    }

    @Override
    public void precommit() {
        //
    }

    @Override
    public void commit() {
        for (EntryEventImpl entryEvent : pendingValue) {
            entryEvent.getRegion().putInternal(entryEvent.getKey(), entryEvent.getValue());
        }
    }

    @Override
    public void rollback() {

    }

    @Override
    public boolean isInProgress() {
        return false;
    }

    @Override
    public void lock() {

    }

    @Override
    public void release() {

    }

    @Override
    public void unlock() {

    }
}
