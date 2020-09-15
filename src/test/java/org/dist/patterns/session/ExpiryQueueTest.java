package org.dist.patterns.session;

import org.apache.zookeeper.common.Time;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

public class ExpiryQueueTest {

    @Test
    public void shouldSetExpirationTimeForGivenKey() throws InterruptedException {
        ExpiryQueue<String> queue = new ExpiryQueue<String>(3000);

        queue.update("sessionId", 1000, Time.currentElapsedTime());
        Thread.sleep(1000);
        queue.update("sessionId1", 1000, Time.currentElapsedTime());
        queue.update("sessionId3", 1000, Time.currentElapsedTime());
        var now = Time.currentElapsedTime();
        long timeout = ((now + 1000) / 3000 + 1) * 3000;

        Set<String> o = (Set<String>) queue.getExpiryMap().get(timeout);
        System.out.println(o.size());
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