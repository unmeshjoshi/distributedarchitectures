package org.dist.patterns.distlog;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.dist.patterns.common.JsonSerDes;
import org.dist.queue.utils.ZkUtils;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class LedgerManager {
    private static final String ledgerRootPath = "/ledgers";
    static public final String LEDGER_NODE_PREFIX = "L";
    static final String AVAILABLE_NODE = "available";
    private final String ledgerPrefix;
    private final ConcurrentHashMap<Long, Boolean> activeLedgers;
    private ZkClient zkClient;

    public LedgerManager(ZkClient zkClient) {
        this.zkClient = zkClient;
        ledgerPrefix = ledgerRootPath + "/" + LEDGER_NODE_PREFIX;
        activeLedgers = new ConcurrentHashMap<Long, Boolean>();
    }

    public String newLedgerPath(final LedgerMetadata metadata) {
        try {
            return ZkUtils.createSequentialPersistentPath(zkClient, ledgerPrefix, JsonSerDes.serialize(metadata));
        } catch(ZkNoNodeException e) {
            String parentPath = ledgerPrefix.substring(0, ledgerPrefix.lastIndexOf('/'));
            ZkUtils.createPersistentPath(zkClient, parentPath, "");
            return ZkUtils.createSequentialPersistentPath(zkClient, ledgerPrefix, JsonSerDes.serialize(metadata));
        }
    }

     public long getLedgerId(String nodeName) throws IOException {
        long ledgerId;
        try {
            String parts[] = nodeName.split(ledgerPrefix);
            ledgerId = Long.parseLong(parts[parts.length - 1]);
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
        return ledgerId;
    }
}