package ru.mail.polis.nekobitlz;

import ru.mail.polis.DAO;

import java.io.Closeable;

public interface TransactionDAO extends Closeable {

    Transaction beginTransaction(String tag);
    void commit();
    void abort();
    DAO getStorage();
}
