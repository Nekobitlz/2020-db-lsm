package ru.mail.polis.nekobitlz;

import ru.mail.polis.DAO;

import java.io.Closeable;

public interface TransactionDAO extends Closeable {

    /**
     * Starts a new transaction.
     *
     * @param tag transaction tag
     * @return created new transaction
     */
    Transaction beginTransaction(String tag);

    /**
     * Applies all transaction changes
     */
    void commit();

    /**
     * Сancels all transaction changes
     */
    void abort();

    /**
     * Returns the data store
     * @return data store
     */
    DAO getStorage();
}
