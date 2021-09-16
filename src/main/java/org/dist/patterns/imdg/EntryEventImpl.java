package org.dist.patterns.imdg;

public class EntryEventImpl {
    private LocalRegion region;
    private Operation update;
    private Object key;
    private Object value;

    public <K, V> EntryEventImpl(LocalRegion region, Operation update, K key, V value) {
        this.region = region;
        this.update = update;
        this.key = key;
        this.value = value;
    }

    public LocalRegion getRegion() {
        return region;
    }

    public Operation getUpdate() {
        return update;
    }

    public Object getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }
}
