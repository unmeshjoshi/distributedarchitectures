package org.dist.patterns.session;

import org.junit.Test;

import static org.junit.Assert.*;

public class SessionTrackerImplTest {
    @Test
    public void testSessionTracker() throws InterruptedException {
        SessionTrackerImpl sessionTracker = new SessionTrackerImpl(3000, 1);
        sessionTracker.start();
        long session = sessionTracker.createSession(10000);
        Thread.sleep(100000);
    }
}