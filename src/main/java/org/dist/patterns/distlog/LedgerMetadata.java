package org.dist.patterns.distlog;

import org.dist.kvstore.InetAddressAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

public class LedgerMetadata {
    static final Logger LOG = LoggerFactory.getLogger(LedgerMetadata.class);

    private static final String closed = "CLOSED";
    private static final String lSplitter = "\n";
    private static final String tSplitter = "\t";

    // can't use -1 for NOTCLOSED because that is reserved for a closed, empty
    // ledger
    public static final int NOTCLOSED = -101;
    public static final int IN_RECOVERY = -102;

    int ensembleSize;
    int quorumSize;
    long length;
    long close;
    private SortedMap<Long, ArrayList<InetAddressAndPort>> ensembles = new TreeMap<Long, ArrayList<InetAddressAndPort>>();
    private ArrayList<InetAddressAndPort> currentEnsemble;

    public LedgerMetadata(int ensembleSize, int quorumSize) {
        this.ensembleSize = ensembleSize;
        this.quorumSize = quorumSize;

        /*
         * It is set in PendingReadOp.readEntryComplete, and
         * we read it in LedgerRecoveryOp.readComplete.
         */
        this.length = 0;
        this.close = NOTCLOSED;
    };

    private LedgerMetadata() {
        this(0, 0);
    }

    /**
     * Get the Map of bookie ensembles for the various ledger fragments
     * that make up the ledger.
     *
     * @return SortedMap of Ledger Fragments and the corresponding
     * bookie ensembles that store the entries.
     */
    public SortedMap<Long, ArrayList<InetAddressAndPort>> getEnsembles() {
        return ensembles;
    }

    boolean isClosed() {
        return close != NOTCLOSED
                && close != IN_RECOVERY;
    }

    boolean isInRecovery() {
        return IN_RECOVERY == close;
    }

    void markLedgerInRecovery() {
        close = IN_RECOVERY;
    }

    void close(long entryId) {
        close = entryId;
    }

    void addEnsemble(long startEntryId, ArrayList<InetAddressAndPort> ensemble) {
        assert ensembles.isEmpty() || startEntryId >= ensembles.lastKey();

        ensembles.put(startEntryId, ensemble);
        currentEnsemble = ensemble;
    }

    ArrayList<InetAddressAndPort> getEnsemble(long entryId) {
        // the head map cannot be empty, since we insert an ensemble for
        // entry-id 0, right when we start
        return ensembles.get(ensembles.headMap(entryId + 1).lastKey());
    }


}
