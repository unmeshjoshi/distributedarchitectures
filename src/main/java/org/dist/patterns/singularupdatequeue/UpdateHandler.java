package org.dist.patterns.singularupdatequeue;

public interface UpdateHandler<U, V> {
    public V update(U u);
}
