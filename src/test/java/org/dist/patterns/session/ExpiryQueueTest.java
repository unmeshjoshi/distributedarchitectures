package org.dist.patterns.session;

import org.apache.zookeeper.common.Time;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

public class ExpiryQueueTest {

    @Test
    public void shouldSetExpirationTimeForGivenKey() {
        ExpiryQueue<String> queue = new ExpiryQueue<String>(100);
        long now = Time.currentElapsedTime();
        queue.update("sessionId", 1000, now);
        long timeout = ((now + 1000) / 100 + 1) * 100;

        Set<String> o = (Set<String>) queue.getExpiryMap().get(timeout);
        assertEquals(o.iterator().next(), "sessionId");
    }
    @Test
    public void shouldExpireElementsAfterTimeout() {
        ExpiryQueue queue = new ExpiryQueue(100);
        queue.update("sessionId", 1000, Time.currentElapsedTime());

        Set expiredElements = queue.poll();
        while(expiredElements.size() == 0) {
            expiredElements = queue.poll();
        }
        assertEquals(expiredElements.size(), 1);
    }
}