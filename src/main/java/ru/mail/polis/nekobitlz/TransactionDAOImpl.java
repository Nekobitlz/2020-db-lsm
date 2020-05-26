package ru.mail.polis.nekobitlz;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;

import java.io.File;
import java.io.IOException;

public class TransactionDAOImpl implements TransactionDAO {

    private final Coordinator coordinator = new Coordinator();
    private final DAO dao;

    public TransactionDAOImpl(@NotNull final File folder, final long bytesFlushThreshold) throws IOException {
        this.dao = new DAOImpl(folder, bytesFlushThreshold);
    }

    @Override
    public Transaction beginTransaction(String tag) {
        if (coordinator.containsTransactionWithTag(tag)) {
            throw new IllegalArgumentException("Transaction with this tag already exists, please select a different tag");
        } else {
            final Transaction transaction = new Transaction(tag, dao, coordinator);
            coordinator.addTransaction(transaction);
            return transaction;
        }
    }

    @Override
    public void commit() {
        coordinator.commitTransactions();
    }

    @Override
    public void abort() {
        coordinator.abortTransactions();
    }

    @Override
    public void close() {
        coordinator.abortTransactions();
    }

    @Override
    public DAO getStorage() {
        return dao;
    }
}
