package org.dist.patterns.imdg;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LocalRegion<K, V> implements Region<K, V> {


    private final EntryEventFactory entryEventFactory = new EntryEventFactory();
    private Object id;
    private TXManagerImpl txMgr;

    public LocalRegion(Object id) {
        this.id = id;
    }

    @Override
    public V get(K key) {
        lock.readLock().lock();
        try {
            return values.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public V put(TransactionId id, K key, V value) {
        TXManagerImpl txMgr = this.getCacheTransactionManager();
        TXStateProxy stateProxy = txMgr.getTransactionState(id);
        try {
            return put(key, value);
        } finally {
            txMgr.release(stateProxy);
        }
    }

    @Override
    public V put(K key, V value) {
        EntryEventImpl entryEvent = newUpdateEntryEvent(key, value);
        basicPut(entryEvent);
        return null;
    }

    public void commit(TransactionId id) {
        TXStateProxy transactionState = txMgr.getTransactionState(id);
        try {
            transactionState.commit();
        } finally {
         txMgr.remove(id, transactionState);
        }
    }


    private void basicPut(EntryEventImpl entryEvent) {
        this.txMgr = this.getCacheTransactionManager();
        getDataView().putEntry(entryEvent);
    }

    private TXManagerImpl getCacheTransactionManager() {
        return TXManagerImpl.currentInstance;
    }

    public InternalDataView getDataView() {
        final TXStateInterface tx = getTXState();
//        if (tx == null) {
//            return sharedDataView;
//        }
        return tx;
    }

    private TXStateInterface getTXState() {
        return TXManagerImpl.getCurrentTXState();
    }

    EntryEventImpl newUpdateEntryEvent(Object key, Object value) {

//        validateArguments(key, value, aCallbackArgument);
//        if (value == null) {
//            throw new NullPointerException(
//                    "value must not be null");
//        }
//        checkReadiness();
//        checkForLimitedOrNoAccess();
        //TODO
//        discoverJTA();

        // This used to call the constructor which took the old value. It
        // was modified to call the other EntryEventImpl constructor so that
        // an id will be generated by default. Null was passed in anyway.
        // generate EventID
        final EntryEventImpl event =
                entryEventFactory.create(this, Operation.UPDATE, key, value, getMyId());
        return event;
    }

    private Object getMyId() {
        return id;
    }

    Map<K, V> values = new HashMap<>();
    public void putInternal(K key, V value) {
       values.put(key, value);
    }

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    public void writeLock() {
        lock.writeLock().lock();
    }

    public void unlock() {
        lock.writeLock().unlock();
    }
}
