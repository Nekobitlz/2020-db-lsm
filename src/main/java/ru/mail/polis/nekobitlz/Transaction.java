package ru.mail.polis.nekobitlz;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.DAO;
import ru.mail.polis.Files;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class Transaction implements DAO {

    private final Logger logger = LoggerFactory.getLogger(Transaction.class);
    private static final ByteBuffer TOMBSTONE = ByteBuffer.allocate(0);

    private final String tag;
    private final DAO changes;
    private final DAO dao;
    private final Coordinator coordinator;
    private boolean isClosed;

    /**
     * Creates a new transaction.
     *
     * @param tag         transaction tag
     * @param dao         storage
     * @param coordinator transaction management coordinator
     */
    public Transaction(final String tag, final DAO dao, final Coordinator coordinator) throws IOException {
        this.tag = tag;
        this.dao = dao;
        this.coordinator = coordinator;
        this.changes = new DAOImpl(coordinator.getFolder(tag), coordinator.getBytesFlushThreshold());
    }

    /**
     * Returns an iterator over the elements in storage and transaction elements.
     *
     * @param from the key with which the iteration begins
     * @return iterator
     * @throws IOException if a write error has occurred
     */
    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        assertClosed();
        final List<Iterator<Record>> iterators = new ArrayList<>();
        iterators.add(changes.iterator(from));
        iterators.add(dao.iterator(from));
        final Iterator<Record> mergedIterator = Iterators.mergeSorted(iterators, Comparator
                .comparing(Record::getKey)
                .thenComparing(Comparator
                        .comparing(Record::getValue)
                        .reversed()
                ));
        final Iterator<Record> collapsedIterator = Iters.collapseEquals(mergedIterator, Record::getKey);

        return Iterators.filter(collapsedIterator, i -> !i.getValue().equals(TOMBSTONE));
    }

    /**
     * Inserts or updates value by given key.
     *
     * @param key   target key
     * @param value target value
     * @throws IOException if a flush error has occurred
     */
    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        assertClosed();
        assertKeyLocked(key);
        coordinator.lockKey(tag, key);
        changes.upsert(key, value);
    }

    /**
     * Removes value by given key.
     *
     * @param key target key
     * @throws IOException if a flush error has occurred
     */
    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        assertClosed();
        assertKeyLocked(key);
        coordinator.lockKey(tag, key);
        changes.upsert(key, TOMBSTONE);
    }

    /**
     * Return value by tag if it is exist.
     *
     * @param key target key
     * @return found value by tag
     * @throws IOException         if can’t get an iterator
     * @throws NoSuchFileException if value not found
     */
    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException {
        assertClosed();
        if (changes.contains(key)) {
            if (changes.get(key).equals(TOMBSTONE)) {
                throw new NoSuchElementException();
            }
        } else {
            return dao.get(key);
        }

        return changes.get(key);
    }

    /**
     * Applies transaction changes.
     */
    public void commit() throws IOException {
        changes.iterator(TOMBSTONE).forEachRemaining((item) -> {
            try {
                if (item.getValue().equals(TOMBSTONE)) {
                    dao.remove(item.getKey());
                } else {
                    dao.upsert(item.getKey(), item.getValue());
                }
            } catch (IOException e) {
                logger.error("Failed to commit " + item, e);
            }
        });
        coordinator.removeTransaction(this);
        close();
    }

    /**
     * Cancels transaction changes.
     */
    public void abort() {
        coordinator.removeTransaction(this);
        close();
    }

    /**
     * Returns transaction tag.
     *
     * @return transaction tag
     */
    public String getTag() {
        return tag;
    }

    @Override
    public void close() {
        try {
            changes.iterator(TOMBSTONE).forEachRemaining((item) -> coordinator.unlockKey(item.getKey()));
        } catch (IOException e) {
            logger.error("Failed to unlock transaction keys", e);
        }
        try {
            Files.recursiveDelete(coordinator.getFolder(tag));
        } catch (IOException e) {
            logger.error("Failed to delete", e);
        }
        isClosed = true;
    }

    private void assertKeyLocked(final ByteBuffer key) {
        if (coordinator.isLockedByAnotherTag(tag, key)) {
            throw new ConcurrentModificationException("This key is already in use");
        }
    }

    private void assertClosed() {
        if (isClosed) {
            throw new IllegalStateException(
                    "This transaction is already closed, you need to open a new one in order to perform operations"
            );
        }
    }
}
