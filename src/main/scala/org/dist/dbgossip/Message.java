package org.dist.dbgossip;

import com.google.common.annotations.VisibleForTesting;
import org.dist.util.Networks;

import java.util.concurrent.atomic.AtomicInteger;

import static org.dist.dbgossip.MonotonicClock.approxTime;

public class Message<T> {
    public Header header;
    public T payload;

    public Message() {
    }

    private Message(Header header, T payload) {
        this.header = header;
        this.payload = payload;
    }
    public static <T> Message<T> out(Verb verb, T payload)
    {
        assert !verb.isResponse();

        return outWithParam(nextId(), verb, 0, payload);
    }

    private static <T> Message<T> outWithParam(long id, Verb verb, long expiresAtNanos, T payload)
    {
        return outWithParam(id, verb, expiresAtNanos, payload, 0);
    }

    private static <T> Message<T> outWithParam(long id, Verb verb, long expiresAtNanos, T payload, int flags)
    {
        if (payload == null)
            throw new IllegalArgumentException();

        InetAddressAndPort from = new InetAddressAndPort(new Networks().ipv4Address(), 8080);
        long createdAtNanos = approxTime.now();
        if (expiresAtNanos == 0)
            expiresAtNanos = verb.expiresAtNanos(createdAtNanos);

        return new Message<T>(new Header(id, verb, from, createdAtNanos, expiresAtNanos, flags), payload);
    }

    /** Sender of the message. */
    public InetAddressAndPort from()
    {
        return header.from;
    }

    private static final long NO_ID = 0L; // this is a valid ID for pre40 nodes

    private static final AtomicInteger nextId = new AtomicInteger(0);

    private static long nextId()
    {
        long id;
        do
        {
            id = nextId.incrementAndGet();
        }
        while (id == NO_ID);

        return id;
    }


    /**
     * id of the request/message. In 4.0+ can be shared between multiple messages of the same logical request,
     * whilst in versions above a new id would be allocated for each message sent.
     */
    public long id()
    {
        return header.id;
    }
    /**
     * WARNING: this is inaccurate for messages from pre40 nodes, which can use 0 as an id (but will do so rarely)
     */
    @VisibleForTesting
    boolean hasId()
    {
        return id() != NO_ID;
    }

    public String serialize() {
        return toJson();
    }

    private String toJson() {
        serializeHeader();
        serializePayload();
        return "";
    }

    private void serializePayload() {

    }

    private void serializeHeader() {
        header.serialize();
    }
}
