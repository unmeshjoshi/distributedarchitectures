package org.dist.patterns.wal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


class WriteAheadLogEntry {
    private final Long entryId;
    private final byte[] data;
    private final Integer entryType;

    public WriteAheadLogEntry(Long entryId, byte[] data, Integer entryType) {
        this.entryId = entryId;
        this.data = data;
        this.entryType = entryType;
    }

    public Long getEntryId() {
        return entryId;
    }

    public byte[] getData() {
        return data;
    }

    public Integer getEntryType() {
        return entryType;
    }

    public ByteBuffer serialize() {
        Integer entrySize = entrySize();
        var bufferSize = entrySize + 4; //4 bytes for record length + walEntry size
        var buffer = ByteBuffer.allocate(bufferSize);
        buffer.clear();
        buffer.putInt(entrySize);
        buffer.putInt(0); //normal entry
        buffer.putLong(entryId);
        buffer.put(data);
        return buffer;
    }

    Integer entrySize() {
        return data.length + WriteAheadLog.sizeOfLong + WriteAheadLog.sizeOfInt; //size of all the fields
    }
}

public class WriteAheadLog {
    private static String logSuffix = ".log";
    private static String logPrefix = "wal";
    private static int firstLogId = 0;
    static int sizeOfInt = 4;
    static int sizeOfLong = 8;
    final RandomAccessFile randomAccessFile;
    final FileChannel fileChannel;

    private WriteAheadLog(File file) {
        try {
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = randomAccessFile.getChannel();

        } catch (FileNotFoundException e) {

            throw new RuntimeException(e);
        }
    }

    public static WriteAheadLog openWAL(Integer startIndex, File walDir) {
        var file = new File(walDir, createFileName(startIndex));
        return new WriteAheadLog(file);
    }

    private static String createFileName(Integer startIndex) {
        return logPrefix + "_" + startIndex + logSuffix;
    }

    public void write(String s) {
        writeEntry(s.getBytes());
    }

    private Long lastLogEntryId = 0l;
    private Map<Long, Long> entryOffsets = new HashMap<Long, Long>();

    public Long writeEntry(byte[] bytes) {
        var logEntryId = lastLogEntryId + 1;
        var logEntry = new WriteAheadLogEntry(logEntryId, bytes, 0);
        var filePosition = writeEntry(logEntry);
        lastLogEntryId = logEntryId;
        entryOffsets.put(logEntryId, filePosition);
        return logEntryId;
    }

    public List<WriteAheadLogEntry> readAll() {
        try {
            fileChannel.position(0);
            var totalBytesRead = 0L;
            var entries = new ArrayList<WriteAheadLogEntry>();
            var deserializer = new WALEntryDeserializer(fileChannel);
            while (totalBytesRead < fileChannel.size()) {
                var startPosition = fileChannel.position();
                WriteAheadLogEntry entry = deserializer.readEntry();
                totalBytesRead += entry.entrySize() + WriteAheadLog.sizeOfInt; //size of entry + size of int which stores length
                entryOffsets.put(entry.getEntryId(), startPosition);
                entries.add(entry);
            }
            return entries;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Long writeEntry(WriteAheadLogEntry entry) {
        var buffer = entry.serialize();
        return writeToChannel(buffer);
    }

    private Long writeToChannel(ByteBuffer buffer) {
        try {
            buffer.flip();
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
            flush();
            return fileChannel.position();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() {
        try {
            fileChannel.force(true);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        flush();

        try {
            fileChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void truncate(Long logIndex) {
        var filePosition = entryOffsets.get(logIndex);
        if (filePosition == null) throw new IllegalArgumentException("No file position available for logIndex=" + logIndex);

        try {
            fileChannel.truncate(filePosition);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}


class WALEntryDeserializer {
    final ByteBuffer intBuffer = ByteBuffer.allocate(WriteAheadLog.sizeOfInt);
    final ByteBuffer longBuffer = ByteBuffer.allocate(WriteAheadLog.sizeOfLong);
    private FileChannel logChannel;

    public WALEntryDeserializer(FileChannel logChannel) {
        this.logChannel = logChannel;
    }

    WriteAheadLogEntry readEntry() {
        Integer entrySize = readInteger();
        Integer entryType = readInteger();
        Long entryId = readLong();

        var dataSize = (entrySize - (WriteAheadLog.sizeOfInt + WriteAheadLog.sizeOfLong));
        ByteBuffer buffer = ByteBuffer.allocate(dataSize);
        var position = readFromChannel(logChannel, buffer);
        var bytesRead = entrySize + WriteAheadLog.sizeOfInt;
        return new WriteAheadLogEntry(entryId, buffer.array(), entryType);
    }

    public Long readLong() {
        long position = readFromChannel(logChannel, longBuffer);
        return longBuffer.getLong();
    }

    public Integer readInteger() {
        var position = readFromChannel(logChannel, intBuffer);
        return intBuffer.getInt();
    }

    private long readFromChannel(FileChannel channel, ByteBuffer buffer) {

        try {
            buffer.clear();//clear to start reading.

            int bytesRead;
            long currentPosition = channel.position();
            do {
                bytesRead = channel.read(buffer, currentPosition);
                currentPosition += bytesRead;
            } while (bytesRead != -1 && buffer.hasRemaining());

            buffer.flip(); //read to be read

            channel.position(currentPosition); //advance channel position
            return channel.position();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

