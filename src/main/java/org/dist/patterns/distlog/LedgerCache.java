package org.dist.patterns.distlog;

import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class LedgerCache {
    private final RocksDB db;


    public LedgerCache(File cacheDir) {
        Options options = new Options();
        // Keep log files for 1month
        options.setKeepLogFileNum(30);
        options.setCreateIfMissing(true);
        options.setLogFileTimeToRoll(TimeUnit.DAYS.toSeconds(1));
        try {
            db = RocksDB.open(options, cacheDir.getPath());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public void putEntryOffset(long ledgerId, long entryId, long location) {
        ByteBuffer keyBuff = ByteBuffer.allocate(16);
        keyBuff.putLong(ledgerId);
        keyBuff.putLong(entryId);
        keyBuff.flip();

        ByteBuffer valueBuff = ByteBuffer.allocate(8);
        valueBuff.putLong(location);
        valueBuff.flip();
        byte[] positionBytes = valueBuff.array();
        try {
            db.put(keyBuff.array(), positionBytes);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() throws RocksDBException {
        FlushOptions options = new FlushOptions();
        db.flush(options);
    }

    public long getEntryOffset(long ledgerId, long entryId) {
        ByteBuffer keyBuff = ByteBuffer.allocate(16);
        keyBuff.putLong(ledgerId);
        keyBuff.putLong(entryId);
        keyBuff.flip();

        try {
            byte[] locationBytes = db.get(keyBuff.array());
            return ByteBuffer.wrap(locationBytes).getLong();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
