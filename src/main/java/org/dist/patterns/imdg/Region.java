package org.dist.patterns.imdg;

public interface Region<K, V> {
    V get(K key);
    V put(K key, V value);
}
