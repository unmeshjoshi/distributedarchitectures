package org.dist.dbgossip;

import java.util.HashMap;
import java.util.Map;

/**
 * Split into a separate object to allow partial message deserialization without wasting work and allocation
 * afterwards, if the entire message is necessary and available.
 */
public class Header {
    public final long id;
    public final Verb verb;
    public final InetAddressAndPort from;
    public final long createdAtNanos;
    public final long expiresAtNanos;
    private final int flags;

    Header(long id, Verb verb, InetAddressAndPort from, long createdAtNanos, long expiresAtNanos, int flags) {
        this.id = id;
        this.verb = verb;
        this.from = from;
        this.createdAtNanos = createdAtNanos;
        this.expiresAtNanos = expiresAtNanos;
        this.flags = flags;
    }

    public String serialize() {
        return toJson();
    }

    private String toJson() {
        Map<String, Object> map = new HashMap();
        map.put("id", id);
        map.put("id", this.from.serialize());
        return "";
    }
}