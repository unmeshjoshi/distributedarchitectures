package org.dist.patterns.session;

import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.EphemeralType;

public class SessionIdGenerator {

    public static long initializeNextSessionId(long id) {
        long nextSid;
        nextSid = (Time.currentElapsedTime() << 24) >>> 8;
        nextSid = nextSid | (id << 56);
        if (nextSid == EphemeralType.CONTAINER_EPHEMERAL_OWNER) {
            ++nextSid;  // this is an unlikely edge case, but check it just in case
        }
        return nextSid;
    }
}
