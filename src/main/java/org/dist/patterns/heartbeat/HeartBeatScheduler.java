package org.dist.patterns.heartbeat;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class HeartBeatScheduler {
    private Runnable action;
    private int heartBeatInterval;

    public HeartBeatScheduler(Runnable action, int heartBeatInterval) {
        this.action = action;
        this.heartBeatInterval = heartBeatInterval;
    }

    private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

    private ScheduledFuture<?> scheduledGossipTask = null;

    public void start() {
        scheduledGossipTask = executor.scheduleWithFixedDelay(new HeartBeatTask(action), heartBeatInterval, heartBeatInterval, TimeUnit.MILLISECONDS);
    }

    private static class HeartBeatTask implements Runnable {
        private Runnable action;

        public HeartBeatTask(Runnable action) {
            this.action = action;
        }

        @Override
        public void run() {
            action.run();
        }
    }
}
