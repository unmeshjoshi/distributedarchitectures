package org.dist.patterns.distlog;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class SortedLedgerStorage {
    AtomicLong size = new AtomicLong(0);
    ConcurrentSkipListMap<EntryKey, EntryKeyValue> memTable = new ConcurrentSkipListMap();
    private double skipListSizeLimit = 64 * 1024 * 1024L;

    public long addEntry(long ledgerId, long entryId, final ByteBuffer entry) throws IOException {
        if (isSizeLimitReached()) {

        }

        EntryKeyValue keyValue = newEntry(ledgerId, entryId, entry);
        memTable.putIfAbsent(keyValue, keyValue);
        size.addAndGet(keyValue.getLength());

//        interleavedLedgerStorage.ledgerCache.updateLastAddConfirmed(ledgerId, lac);
        return entryId;
    }

    boolean isSizeLimitReached() {
        return size.get() >= skipListSizeLimit;
    }

    private EntryKeyValue newEntry(long ledgerId, long entryId, final ByteBuffer entry) {
        byte[] buf;
        int offset = 0;
        int length = entry.remaining();
        buf = new byte[length];
        entry.get(buf);
        return new EntryKeyValue(ledgerId, entryId, buf, offset, length);
    }
}
