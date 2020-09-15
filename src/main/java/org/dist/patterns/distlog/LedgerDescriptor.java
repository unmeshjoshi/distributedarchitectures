package org.dist.patterns.distlog;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LedgerDescriptor {
    private long ledgerId;
    private EntryLogger entryLogger;
    private LedgerCache ledgerCache;

    public LedgerDescriptor(long ledgerId, EntryLogger entryLogger, LedgerCache ledgerCache) {
        this.ledgerId = ledgerId;
        this.entryLogger = entryLogger;
        this.ledgerCache = ledgerCache;
    }

    long addEntry(ByteBuffer entry) throws IOException {
        long ledgerId = entry.getLong();

        if (ledgerId != this.ledgerId) {
            throw new IOException("Entry for ledger " + ledgerId + " was sent to " + this.ledgerId);
        }
        long entryId = entry.getLong();
        entry.rewind();

        /*
         * Log the entry
         */
        long pos = entryLogger.addEntry(ledgerId, entry);


        /*
         * Set offset of entry id to be the current ledger position
         */
        ledgerCache.putEntryOffset(ledgerId, entryId, pos);
        return entryId;
    }

    ByteBuffer readEntry(long entryId) throws Exception {
        long offset = ledgerCache.getEntryOffset(ledgerId, entryId);
        if (offset == 0) {
            throw new Bookie.NoEntryException(ledgerId, entryId);
        }
        return ByteBuffer.wrap(entryLogger.readEntry(ledgerId, entryId, offset));
    }
}
