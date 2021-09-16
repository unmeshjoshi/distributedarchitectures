package org.dist.patterns.imdg;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TXManagerImpl {
    static TXManagerImpl currentInstance = new TXManagerImpl();
    private final ThreadLocal<TXStateProxy> txContext = new ThreadLocal<>();

    public static TXStateInterface getCurrentTXState() {
        return currentInstance.getTXState();

    }

    private TXStateInterface getTXState() {
        TXStateProxy tsp = txContext.get();
//        if (tsp == PAUSED) {
//            // treats paused transaction as no transaction.
//            return null;
//        }
//        if (tsp != null && !tsp.isInProgress()) {
//            this.txContext.set(null);
//            tsp = null;
//        }
        return tsp;
    }

    private final Map<TransactionId, TXStateProxy> hostedTXStates = new HashMap<>();

    public TXStateProxy getTransactionState(TransactionId id) {
        TXStateProxy txStateProxy = hostedTXStates.get(id);
        if (txStateProxy == null) {
            txStateProxy = new TXStateProxyImpl(id);
            hostedTXStates.put(id, txStateProxy);
        }
        txStateProxy.lock();//
        setTXState(txStateProxy);
        return txStateProxy;
    }

    private void setTXState(TXStateProxy proxy) {
        txContext.set(proxy);
    }

    private TransactionId getNewTXId() {
        return new TransactionId(UUID.randomUUID());
    }

    public void commit() {
        txContext.get().commit();
    }

    public void release(TXStateProxy txStateProxy) {
        setTXState(null);
        txStateProxy.unlock();//
    }

    public void remove(TransactionId id, TXStateProxy transactionState) {
        release(transactionState);
        hostedTXStates.remove(id);
    }
}
