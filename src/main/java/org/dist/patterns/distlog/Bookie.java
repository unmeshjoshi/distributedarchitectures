package org.dist.patterns.distlog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Bookie {
    public static final long METAENTRY_ID_FORCE_LEDGER  = -0x4000;
    private Map<Long, LedgerDescriptor> ledgers = new HashMap<Long, LedgerDescriptor>();
    private EntryLogger entryLogger;
    private LedgerCache ledgerCache;
    public Bookie(File journalDir, File ledgerDirName, File cachePath) {
        try {
            entryLogger = new EntryLogger(this, ledgerDirName);
            ledgerCache = new LedgerCache(cachePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void addEntry(long ledgerId, ByteBuffer buffer) throws IOException {
        LedgerDescriptor ledgerHandle = getHandle(ledgerId);
        ledgerHandle.addEntry(buffer);
    }

    public ByteBuffer readEntry(long ledgerId, long entryId) throws Exception {
        LedgerDescriptor ledgerHandle = getHandle(ledgerId);
        return ledgerHandle.readEntry(entryId);
    }


    private LedgerDescriptor getHandle(long ledgerId) {
        LedgerDescriptor ledgerHandle = ledgers.get(ledgerId);
        if (ledgerHandle == null) {
            ledgerHandle = new LedgerDescriptor(ledgerId, entryLogger, ledgerCache);
            ledgers.put(ledgerId, ledgerHandle);
        }
        return ledgerHandle;
    }


    public static class NoEntryException extends Exception {
        private long ledgerId;
        private long entryId;

        public NoEntryException(long ledgerId, long entryId) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }
    }
}
