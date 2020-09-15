package org.dist.patterns.session;

import org.dist.queue.TestUtils;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public class SessionTrackerImplTest {
    @Test
    public void testSessionTracker() throws InterruptedException {
        SessionTrackerImpl sessionTracker = new SessionTrackerImpl(3000, 1);
        sessionTracker.start();
        long session = sessionTracker.createSession(1000);
        sessionTracker.touchSession(session, 1000);
        TestUtils.waitUntilTrue(()-> sessionTracker.getSessionsById().size() == 0,
                ()->"waiting for session to expire", 1000L, 100);
    }
}