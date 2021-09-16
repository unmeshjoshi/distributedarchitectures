package org.dist.patterns.imdg;

public class EntryEventFactory {
    public <K, V> EntryEventImpl create(LocalRegion region, Operation update, K key, V value, Object myId) {
        return new EntryEventImpl(region, update, key, value);
    }
}
