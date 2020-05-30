package ru.mail.polis.nekobitlz;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Coordinator {

    private final Map<String, Transaction> transactions = new HashMap<>();
    private final Map<ByteBuffer, String> lockedKeys = new HashMap<>();
    private final File folder;
    private final long bytesFlushThreshold;

    /**
     * Creates a coordinator for transaction management.
     *
     * @param folder folder where can store temporary files
     * @param bytesFlushThreshold MemTable size threshold
     */
    public Coordinator(final File folder, final long bytesFlushThreshold) {
        this.folder = folder;
        this.bytesFlushThreshold = bytesFlushThreshold;
    }

    /**
     * Adds transaction to transaction list.
     * @param transaction target transaction
     */
    public void addTransaction(final Transaction transaction) {
        transactions.put(transaction.getTag(), transaction);
    }

    /**
     * Removes transaction from transaction list.
     * @param transaction target transaction
     */
    public void removeTransaction(final Transaction transaction) {
        transactions.remove(transaction.getTag());
    }

    /**
     * Locks key by transaction tag.
     * @param tag transaction tag
     * @param key target key
     */
    public void lockKey(final String tag, final ByteBuffer key) {
        lockedKeys.put(key, tag);
    }

    /**
     * Unlocks key by transaction tag.
     * @param key target key
     */
    public void unlockKey(final ByteBuffer key) {
        lockedKeys.remove(key);
    }

    /**
     * Checks if the key is locked for this tag.
     *
     * @param key target key
     * @param tag transaction tag to verify
     */
    public boolean isLockedByAnotherTag(final String tag, final ByteBuffer key) {
        final String lockedTag = lockedKeys.get(key);
        if (lockedTag == null) {
            return false;
        }
        return !lockedTag.equals(tag);
    }

    /**
     * Checks if a transaction exists with this tag.
     *
     * @param tag target tag
     */
    public boolean containsTransactionWithTag(final String tag) {
        return transactions.containsKey(tag);
    }

    /**
     * Returns folder where can store temporary files.
     * @param tag target tag
     * @return folder
     */
    public File getFolder(final String tag) {
        File newFolder = new File(folder.getAbsolutePath() + File.separator + tag);
        if (!newFolder.exists()) {
            newFolder.mkdirs();
        }
        return newFolder;
    }

    /**
     * Returns MemTable size threshold.
     * @return bytesFlushThreshold
     */
    public long getBytesFlushThreshold() {
        return bytesFlushThreshold;
    }

    public void close() {
        transactions.forEach((key, value) -> value.close());
        transactions.clear();
    }
}
