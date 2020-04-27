package org.dist.patterns.wal;

import org.dist.queue.TestUtils;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class WriteAheadLogTest {
    @Test
    public void shouldCreateLogFileForGivenIndex() {
        WriteAheadLog testWriteAheadLog = WriteAheadLog.openWAL(1, TestUtils.tempDir("testWal"));
        assertNotNull(testWriteAheadLog.randomAccessFile);
        testWriteAheadLog.close();
    }

    @Test
    public void shouldWriteAndReadEntries() {
        File walDir = TestUtils.tempDir("testWal");

        WriteAheadLog testWriteAheadLog = WriteAheadLog.openWAL(1, walDir);
        testWriteAheadLog.writeEntry("test content".getBytes());
        testWriteAheadLog.writeEntry("test content2".getBytes());
        testWriteAheadLog.close();

        WriteAheadLog readWriteAheadLog = WriteAheadLog.openWAL(1, walDir);
        List<WriteAheadLogEntry> entries = readWriteAheadLog.readAll();

        assertEquals(2, entries.size());
        assertEquals("test content", new String(entries.get(0).getData()));
        assertEquals("test content2", new String(entries.get(1).getData()));
    }

    @Test
    public void shouldTruncateLogAtGivenLogIndex() {
        File walDir = TestUtils.tempDir("testWal");

        WriteAheadLog testWriteAheadLog = WriteAheadLog.openWAL(1, walDir);
        testWriteAheadLog.writeEntry("test content".getBytes());
        testWriteAheadLog.writeEntry("test content2".getBytes());
        testWriteAheadLog.writeEntry("test content3".getBytes());
        testWriteAheadLog.close();

        WriteAheadLog readWriteAheadLog = WriteAheadLog.openWAL(1, walDir);
        List<WriteAheadLogEntry> entries = readWriteAheadLog.readAll();

        assertEquals(3, entries.size());

        readWriteAheadLog.truncate(2l);
        readWriteAheadLog = WriteAheadLog.openWAL(1, walDir);
        assertEquals(1, readWriteAheadLog.readAll().size());
    }

}