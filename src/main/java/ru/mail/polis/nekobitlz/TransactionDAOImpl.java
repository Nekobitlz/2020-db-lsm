package ru.mail.polis.nekobitlz;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;

import java.io.File;
import java.io.IOException;

public class TransactionDAOImpl implements TransactionDAO {

    private final Coordinator coordinator;
    private final DAO dao;

    public TransactionDAOImpl(@NotNull final File folder, final long bytesFlushThreshold) throws IOException {
        this.dao = new DAOImpl(folder, bytesFlushThreshold);
        this.coordinator = new Coordinator(folder, bytesFlushThreshold);
    }

    @Override
    public Transaction beginTransaction(final String tag) {
        if (coordinator.containsTransactionWithTag(tag)) {
            throw new IllegalArgumentException(
                    "Transaction with this tag already exists, please select a different tag"
            );
        } else {
            final Transaction transaction = new Transaction(tag, dao, coordinator);
            coordinator.addTransaction(transaction);
            return transaction;
        }
    }

    @Override
    public void close() {
    }

    @Override
    public DAO getStorage() {
        return dao;
    }
}
