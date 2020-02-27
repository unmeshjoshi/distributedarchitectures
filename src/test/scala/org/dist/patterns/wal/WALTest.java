package org.dist.patterns.wal;

import org.dist.queue.TestUtils;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class WALTest {
    @Test
    public void shouldCreateLogFileForGivenIndex() {
        WAL testWal = WAL.openWAL(1, TestUtils.tempDir("testWal"));
        assertNotNull(testWal.randomAccessFile);
        testWal.close();
    }

    @Test
    public void shouldWriteAndReadEntries() {
        File walDir = TestUtils.tempDir("testWal");

        WAL testWal = WAL.openWAL(1, walDir);
        testWal.writeEntry("test content".getBytes());
        testWal.writeEntry("test content2".getBytes());
        testWal.close();

        WAL readWal = WAL.openWAL(1, walDir);
        List<WALEntry> entries = readWal.readAll();

        assertEquals(2, entries.size());
        assertEquals("test content", new String(entries.get(0).getData()));
        assertEquals("test content2", new String(entries.get(1).getData()));

    }

}