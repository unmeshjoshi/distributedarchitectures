package org.dist.bookkeeper;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.Recycler;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.dist.kvstore.InetAddressAndPort;
import org.dist.patterns.distlog.Bookie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * A writer callback interface.
 */


public class Journal extends Thread {
    public interface WriteCallback {
        void writeComplete(int rc, long ledgerId, long entryId, InetAddressAndPort addr, Object ctx);
    }

    static final long MB = 1024 * 1024L;
    static final int KB = 1024;

    private static final Logger LOG = LoggerFactory.getLogger(Journal.class);
//    private final JournalStats journalStats;
    private final LastLogMark lastLogMark = new LastLogMark(0, 0);
    private final ExecutorService cbThreadPool = Executors.newFixedThreadPool(1,
            new DefaultThreadFactory("bookie-journal-callback"));;
    final File journalDirectory;
    private static final RecyclableArrayList.Recycler<QueueEntry> entryListRecycler =
            new RecyclableArrayList.Recycler<QueueEntry>();
    final ForceWriteThread forceWriteThread;
    private final int journalFormatVersionToWrite = 6;
    private long bufferedEntriesThreshold = 0;
    private long bufferedWritesThreshold = 512 * 1024;
    // Should data be fsynced on disk before triggering the callback
    private final boolean syncData = true;
    private long maxJournalSize = 2 * 1024 * MB;
    private long journalPageCacheFlushIntervalMSec = 1000;
    private boolean running = false;

    public Journal(File journalDirectory) {
        this.journalDirectory = journalDirectory;
        this.forceWriteThread = new ForceWriteThread(this, true);
    }
    // pre-allocation size for the journal files
    final long journalPreAllocSize = 16 * MB;
    // write buffer size for the journal files
    final int journalWriteBufferSize = 64 * KB;
    // number journal files kept before marked journal
    private final boolean removePagesFromCache = true;
    // Time after which we will stop grouping and issue the flush
    private final long maxGroupWaitInNanos = TimeUnit.MILLISECONDS.toNanos(2);;
    // should we flush if the queue is empty
    private final boolean flushWhenQueueEmpty = maxGroupWaitInNanos <= 0;

    @Override
    public void run() {
        this.running = true;
        LOG.info("Starting journal on {}", journalDirectory);

        RecyclableArrayList<QueueEntry> toFlush = entryListRecycler.newInstance();
        int numEntriesToFlush = 0;
        ByteBuf lenBuff = Unpooled.buffer(4);
        int journalAlignmentSize = 512;
        ByteBuf paddingBuff = Unpooled.buffer(2 * journalAlignmentSize);
        paddingBuff.writeZero(paddingBuff.capacity());

        BufferedChannel bc = null;
        JournalChannel logFile = null;
        forceWriteThread.start();
        long batchSize = 0;
        try {
            List<Long> journalIds = Collections.emptyList();
            // Should not use MathUtils.now(), which use System.nanoTime() and
            // could only be used to measure elapsed time.
            // http://docs.oracle.com/javase/1.5.0/docs/api/java/lang/System.html#nanoTime%28%29
            long logId = journalIds.isEmpty() ? System.currentTimeMillis() : journalIds.get(journalIds.size() - 1);
            long lastFlushPosition = 0;
            boolean groupWhenTimeout = false;

            long dequeueStartTime = 0L;
            long lastFlushTimeMs = System.currentTimeMillis();

            QueueEntry qe = null;
            while (true) {
                // new journal file to write
                if (null == logFile) {
                    logId = logId + 1;

                    logFile = new JournalChannel(journalDirectory, logId, journalPreAllocSize, journalWriteBufferSize,
                            journalAlignmentSize, removePagesFromCache,
                            journalFormatVersionToWrite, getBufferedChannelBuilder());


                    bc = logFile.getBufferedChannel();

                    lastFlushPosition = bc.position();
                }

                if (qe == null) {
                    if (numEntriesToFlush == 0) {
                        qe = queue.take();
                        dequeueStartTime = MathUtils.nowInNano();

                    } else {
                        long pollWaitTimeNanos = maxGroupWaitInNanos
                                - MathUtils.elapsedNanos(toFlush.get(0).enqueueTime);
                        if (flushWhenQueueEmpty || pollWaitTimeNanos < 0) {
                            pollWaitTimeNanos = 0;
                        }
                        qe = queue.poll(pollWaitTimeNanos, TimeUnit.NANOSECONDS);
                        dequeueStartTime = MathUtils.nowInNano();
                        boolean shouldFlush = false;
                        // We should issue a forceWrite if any of the three conditions below holds good
                        // 1. If the oldest pending entry has been pending for longer than the max wait time
                        if (maxGroupWaitInNanos > 0 && !groupWhenTimeout && (MathUtils
                                .elapsedNanos(toFlush.get(0).enqueueTime) > maxGroupWaitInNanos)) {
                            groupWhenTimeout = true;
                        } else if (maxGroupWaitInNanos > 0 && groupWhenTimeout
                                && (qe == null // no entry to group
                                || MathUtils.elapsedNanos(qe.enqueueTime) < maxGroupWaitInNanos)) {
                            // when group timeout, it would be better to look forward, as there might be lots of
                            // entries already timeout
                            // due to a previous slow write (writing to filesystem which impacted by force write).
                            // Group those entries in the queue
                            // a) already timeout
                            // b) limit the number of entries to group
                            groupWhenTimeout = false;
                            shouldFlush = true;
                        }else if (qe != null
                                && ((bufferedEntriesThreshold > 0 && toFlush.size() > bufferedEntriesThreshold)
                                || (bc.position() > lastFlushPosition + bufferedWritesThreshold))) {
                            // 2. If we have buffered more than the buffWriteThreshold or bufferedEntriesThreshold
                            groupWhenTimeout = false;
                            shouldFlush = true;

                        } else if (qe == null && flushWhenQueueEmpty) {
                            // We should get here only if we flushWhenQueueEmpty is true else we would wait
                            // for timeout that would put is past the maxWait threshold
                            // 3. If the queue is empty i.e. no benefit of grouping. This happens when we have one
                            // publish at a time - common case in tests.
                            groupWhenTimeout = false;
                            shouldFlush = true;
                        }

                        // toFlush is non null and not empty so should be safe to access getFirst
                        if (shouldFlush) {
                            if (journalFormatVersionToWrite >= JournalChannel.V5) {
                                writePaddingBytes(logFile, paddingBuff, journalAlignmentSize);
                            }

                            bc.flush();

                            for (int i = 0; i < toFlush.size(); i++) {
                                QueueEntry entry = toFlush.get(i);
                                if (entry != null && (!syncData || entry.ackBeforeSync)) {
                                    toFlush.set(i, null);
                                    numEntriesToFlush--;
                                    cbThreadPool.execute(entry);
                                }
                            }

                            lastFlushPosition = bc.position();

                            // Trace the lifetime of entries through persistence
                            if (LOG.isDebugEnabled()) {
                                for (QueueEntry e : toFlush) {
                                    if (e != null) {
                                        LOG.debug("Written and queuing for flush Ledger: {}  Entry: {}",
                                                e.ledgerId, e.entryId);
                                    }
                                }
                            }



                            boolean shouldRolloverJournal = (lastFlushPosition > maxJournalSize);
                            // Trigger data sync to disk in the "Force-Write" thread.
                            // Trigger data sync to disk has three situations:
                            // 1. journalSyncData enabled, usually for SSD used as journal storage
                            // 2. shouldRolloverJournal is true, that is the journal file reaches maxJournalSize
                            // 3. if journalSyncData disabled and shouldRolloverJournal is false, we can use
                            //   journalPageCacheFlushIntervalMSec to control sync frequency, preventing disk
                            //   synchronize frequently, which will increase disk io util.
                            //   when flush interval reaches journalPageCacheFlushIntervalMSec (default: 1s),
                            //   it will trigger data sync to disk
                            if (syncData
                                    || shouldRolloverJournal
                                    || (System.currentTimeMillis() - lastFlushTimeMs
                                    >= journalPageCacheFlushIntervalMSec)) {
                                forceWriteRequests.put(createForceWriteRequest(logFile, logId, lastFlushPosition,
                                        toFlush, shouldRolloverJournal, false));
                                lastFlushTimeMs = System.currentTimeMillis();
                            }
                            toFlush = entryListRecycler.newInstance();
                            numEntriesToFlush = 0;

                            batchSize = 0L;
                            // check whether journal file is over file limit
                            if (shouldRolloverJournal) {
                                // if the journal file is rolled over, the journal file will be closed after last
                                // entry is force written to disk.
                                logFile = null;
                                continue;
                            }
                        }
                    }
                }
                if (!running) {
                    LOG.info("Journal Manager is asked to shut down, quit.");
                    break;
                }
                if (qe == null) { // no more queue entry
                    continue;
                }

                if (qe.entryId != Bookie.METAENTRY_ID_FORCE_LEDGER) {
                    int entrySize = qe.entry.readableBytes();

                    batchSize += (4 + entrySize);

                    lenBuff.clear();
                    lenBuff.writeInt(entrySize);

                    // preAlloc based on size
                    logFile.preAllocIfNeeded(4 + entrySize);

                    bc.write(lenBuff);
                    bc.write(qe.entry);
                    qe.entry.release();
                }

                toFlush.add(qe);
                numEntriesToFlush++;
                qe = null;
            }
        }  catch (IOException | InterruptedException ioe) {
            LOG.error("I/O exception in Journal thread!", ioe);
        }  finally {
            // There could be packets queued for forceWrite on this logFile
            // That is fine as this exception is going to anyway take down the
            // the bookie. If we execute this as a part of graceful shutdown,
            // close will flush the file system cache making any previous
            // cached writes durable so this is fine as well.
            try {
                bc.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    static final int PADDING_MASK = -0x100;

    static void writePaddingBytes(JournalChannel jc, ByteBuf paddingBuffer, int journalAlignSize)
            throws IOException {
        int bytesToAlign = (int) (jc.bc.position() % journalAlignSize);
        if (0 != bytesToAlign) {
            int paddingBytes = journalAlignSize - bytesToAlign;
            if (paddingBytes < 8) {
                paddingBytes = journalAlignSize - (8 - paddingBytes);
            } else {
                paddingBytes -= 8;
            }
            paddingBuffer.clear();
            // padding mask
            paddingBuffer.writeInt(PADDING_MASK);
            // padding len
            paddingBuffer.writeInt(paddingBytes);
            // padding bytes
            paddingBuffer.writerIndex(paddingBuffer.writerIndex() + paddingBytes);

            jc.preAllocIfNeeded(paddingBuffer.readableBytes());
            // write padding bytes
            jc.bc.write(paddingBuffer);
        }
    }

    private final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    public BufferedChannelBuilder getBufferedChannelBuilder() {
        return (FileChannel fc, int capacity) -> new BufferedChannel(allocator, fc, capacity);
    }


    /**
     * For testability.
     */
    @FunctionalInterface
    public interface BufferedChannelBuilder {
        BufferedChannelBuilder DEFAULT_BCBUILDER = (FileChannel fc,
                                                    int capacity) -> new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, capacity);

        BufferedChannel create(FileChannel fc, int capacity) throws IOException;
    }
    // journal entry queue to commit
    final BlockingQueue<QueueEntry> queue = new ArrayBlockingQueue<>(10000);;
    final BlockingQueue<ForceWriteRequest> forceWriteRequests = new ArrayBlockingQueue<>(10000);;

    public void logAddEntry(long ledgerId, long entryId, ByteBuf entry,
                            boolean ackBeforeSync, WriteCallback cb, Object ctx) throws InterruptedException {
        entry.retain();

         queue.put(QueueEntry.create(
                entry, ackBeforeSync,  1, entryId, cb, ctx, MathUtils.nowInNano()));
    }

    /**
     * Token which represents the need to force a write to the Journal.
     */
    @VisibleForTesting
    public class ForceWriteRequest {
        private JournalChannel logFile;
        private RecyclableArrayList<QueueEntry> forceWriteWaiters;
        private boolean shouldClose;
        private boolean isMarker;
        private long lastFlushedPosition;
        private long logId;
        private long enqueueTime;

        public int process(boolean shouldForceWrite) throws IOException {
            if (isMarker) {
                return 0;
            }

            try {
                if (shouldForceWrite) {
                    long startTime = MathUtils.nowInNano();
                    this.logFile.forceWrite(false);
                }
                lastLogMark.setCurLogMark(this.logId, this.lastFlushedPosition);

                // Notify the waiters that the force write succeeded
                for (int i = 0; i < forceWriteWaiters.size(); i++) {
                    QueueEntry qe = forceWriteWaiters.get(i);
                    if (qe != null) {
                        cbThreadPool.execute(qe);
                    }
//                    journalStats.getJournalCbQueueSize().inc();
                }

                return forceWriteWaiters.size();
            } finally {
                closeFileIfNecessary();
            }
        }

        public void closeFileIfNecessary() {
            // Close if shouldClose is set
            if (shouldClose) {
                // We should guard against exceptions so its
                // safe to call in catch blocks
                try {
                    logFile.close();
                    // Call close only once
                    shouldClose = false;
                } catch (IOException ioe) {
                    LOG.error("I/O exception while closing file", ioe);
                }
            }
        }

        private final Recycler.Handle<ForceWriteRequest> recyclerHandle;

        private ForceWriteRequest(Recycler.Handle<ForceWriteRequest> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private void recycle() {
            logFile = null;
            if (forceWriteWaiters != null) {
                forceWriteWaiters.recycle();
                forceWriteWaiters = null;
            }
            recyclerHandle.recycle(this);
        }
    }

    private ForceWriteRequest createForceWriteRequest(JournalChannel logFile,
                                                      long logId,
                                                      long lastFlushedPosition,
                                                      RecyclableArrayList<QueueEntry> forceWriteWaiters,
                                                      boolean shouldClose,
                                                      boolean isMarker) {
        ForceWriteRequest req = forceWriteRequestsRecycler.get();
        req.forceWriteWaiters = forceWriteWaiters;
        req.logFile = logFile;
        req.logId = logId;
        req.lastFlushedPosition = lastFlushedPosition;
        req.shouldClose = shouldClose;
        req.isMarker = isMarker;
        req.enqueueTime = MathUtils.nowInNano();
//        journalStats.getForceWriteQueueSize().inc();
        return req;
    }

    private final Recycler<ForceWriteRequest> forceWriteRequestsRecycler = new Recycler<ForceWriteRequest>() {
        protected ForceWriteRequest newObject(
                Recycler.Handle<ForceWriteRequest> handle) {
            return new ForceWriteRequest(handle);
        }
    };



    private static class QueueEntry implements Runnable {
        ByteBuf entry;
        long ledgerId;
        long entryId;
        WriteCallback cb;
        Object ctx;
        long enqueueTime;
        boolean ackBeforeSync;

        static QueueEntry create(ByteBuf entry, boolean ackBeforeSync, long ledgerId, long entryId,
                                 WriteCallback cb, Object ctx, long enqueueTime) {
            QueueEntry qe = RECYCLER.get();
            qe.entry = entry;
            qe.ackBeforeSync = ackBeforeSync;
            qe.cb = cb;
            qe.ctx = ctx;
            qe.ledgerId = ledgerId;
            qe.entryId = entryId;
            qe.enqueueTime = enqueueTime;
            return qe;
        }

        @Override
        public void run() {
            cb.writeComplete(0, ledgerId, entryId, null, ctx);
            recycle();
        }

        private final Recycler.Handle<QueueEntry> recyclerHandle;

        private QueueEntry(Recycler.Handle<QueueEntry> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<QueueEntry> RECYCLER = new Recycler<QueueEntry>() {
            protected QueueEntry newObject(Recycler.Handle<QueueEntry> handle) {
                return new QueueEntry(handle);
            }
        };

        private void recycle() {
            recyclerHandle.recycle(this);
        }
    }
    /**
     * Last Log Mark.
     */
    public class LastLogMark {
        private final LogMark curMark;

        LastLogMark(long logId, long logPosition) {
            this.curMark = new LogMark(logId, logPosition);
        }

        void setCurLogMark(long logId, long logPosition) {
            curMark.setLogMark(logId, logPosition);
        }

        LastLogMark markLog() {
            return new LastLogMark(curMark.getLogFileId(), curMark.getLogFileOffset());
        }

        public LogMark getCurMark() {
            return curMark;
        }

        @Override
        public String toString() {
            return curMark.toString();
        }
    }

    /**
     * ForceWriteThread is a background thread which makes the journal durable periodically.
     *
     */
    private class ForceWriteThread extends Thread {
        volatile boolean running = true;
        // This holds the queue entries that should be notified after a
        // successful force write
        Thread threadToNotifyOnEx;
        // should we group force writes
        private final boolean enableGroupForceWrites;
        // make flush interval as a parameter
        public ForceWriteThread(Thread threadToNotifyOnEx, boolean enableGroupForceWrites) {
            super("ForceWriteThread");
            this.threadToNotifyOnEx = threadToNotifyOnEx;
            this.enableGroupForceWrites = enableGroupForceWrites;
        }
        @Override
        public void run() {
            LOG.info("ForceWrite Thread started");

            boolean shouldForceWrite = true;
            int numReqInLastForceWrite = 0;
            while (running) {
                ForceWriteRequest req = null;
                try {
                    req = forceWriteRequests.take();
                    // Force write the file and then notify the write completions
                    //
                    if (!req.isMarker) {
                        if (shouldForceWrite) {
                            // if we are going to force write, any request that is already in the
                            // queue will benefit from this force write - post a marker prior to issuing
                            // the flush so until this marker is encountered we can skip the force write
                            if (enableGroupForceWrites) {
                                forceWriteRequests.put(createForceWriteRequest(req.logFile, 0, 0, null, false, true));
                            }

                            // If we are about to issue a write, record the number of requests in
                            // the last force write and then reset the counter so we can accumulate
                            // requests in the write we are about to issue
                            if (numReqInLastForceWrite > 0) {
//                                journalStats.getForceWriteGroupingCountStats()
//                                        .registerSuccessfulValue(numReqInLastForceWrite);
                                numReqInLastForceWrite = 0;
                            }
                        }
                    }
                    numReqInLastForceWrite += req.process(shouldForceWrite);

                    if (enableGroupForceWrites
                            // if its a marker we should switch back to flushing
                            && !req.isMarker
                            // This indicates that this is the last request in a given file
                            // so subsequent requests will go to a different file so we should
                            // flush on the next request
                            && !req.shouldClose) {
                        shouldForceWrite = false;
                    } else {
                        shouldForceWrite = true;
                    }
                } catch (IOException ioe) {
                    LOG.error("I/O exception in ForceWrite thread", ioe);
                    running = false;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.info("ForceWrite thread interrupted");
                    // close is idempotent
                    if (null != req) {
                        req.shouldClose = true;
                        req.closeFileIfNecessary();
                    }
                    running = false;
                } finally {
                    if (req != null) {
                        req.recycle();
                    }
                }
            }
            // Regardless of what caused us to exit, we should notify the
            // the parent thread as it should either exit or be in the process
            // of exiting else we will have write requests hang
            threadToNotifyOnEx.interrupt();
        }
        // shutdown sync thread
        void shutdown() throws InterruptedException {
            running = false;
            this.interrupt();
            this.join();
        }
    }
}
