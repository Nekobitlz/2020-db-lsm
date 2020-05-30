package ru.mail.polis.nekobitlz;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.DAO;

import java.io.File;
import java.io.IOException;

public class TransactionDAOImpl implements TransactionDAO {

    private final Logger logger = LoggerFactory.getLogger(TransactionDAOImpl.class);
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
            final Transaction transaction;
            try {
                transaction = new Transaction(tag, dao, coordinator);
                coordinator.addTransaction(transaction);
                return transaction;
            } catch (IOException e) {
                logger.error("Failed to create transaction", e);
                return null;
            }
        }
    }

    @Override
    public void close() {
        coordinator.close();
    }

    @Override
    public DAO getStorage() {
        return dao;
    }
}
