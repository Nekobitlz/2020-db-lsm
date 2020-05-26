package ru.mail.polis.nekobitlz;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

public class Coordinator {

    private final Map<String, Transaction> transactions = new HashMap<>();
    private final Map<ByteBuffer, String> lockedKeys = new HashMap<>();
    private final File folder;
    private final long bytesFlushThreshold;

    public Coordinator(final File folder, final long bytesFlushThreshold) {
        this.folder = folder;
        this.bytesFlushThreshold = bytesFlushThreshold;
    }

    public void addTransaction(final Transaction transaction) {
        transactions.put(transaction.getTag(), transaction);
    }

    public void removeTransaction(final Transaction transaction) {
        transactions.remove(transaction.getTag());
    }

    public void lockKey(final String tag, final ByteBuffer key) {
        lockedKeys.put(key, tag);
    }

    public void unlockKey(final ByteBuffer key) {
        lockedKeys.remove(key);
    }

    public boolean isLockedByTag(final String tag, final ByteBuffer key) {
        final String lockedTag = lockedKeys.get(key);
        if (lockedTag == null) {
            return false;
        }
        return !lockedTag.equals(tag);
    }

    public boolean containsTransactionWithTag(String tag) {
        return transactions.containsKey(tag);
    }

    public void commitTransactions() {
        transactions.forEach((key, value) -> value.commit());
    }

    public void abortTransactions() {
        transactions.forEach((key, value) -> value.abort());
    }

    public File getFolder(String tag) {
        return new File(folder.getAbsolutePath() + tag);
    }

    public long getBytesFlushThreshold() {
        return bytesFlushThreshold;
    }
}
