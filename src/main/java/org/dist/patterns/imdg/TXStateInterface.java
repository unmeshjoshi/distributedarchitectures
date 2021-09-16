package org.dist.patterns.imdg;

public interface TXStateInterface extends InternalDataView {
    TransactionId getTransactionId();
    /*
     * Only applicable for Distributed transaction.
     */
    void precommit();

    void commit();

    void rollback();

}
