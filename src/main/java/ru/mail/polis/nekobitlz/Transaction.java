package ru.mail.polis.nekobitlz;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.*;

public class Transaction implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(Transaction.class);
    private static final ByteBuffer TOMBSTONE = ByteBuffer.allocate(0);

    private final String tag;
    private final MemTable changes;
    private final DAO dao;
    private final Coordinator coordinator;
    private boolean isClosed;

    /**
     * Creates a new transaction
     *
     * @param tag transaction tag
     * @param dao storage
     * @param coordinator transaction management coordinator
     */
    public Transaction(final String tag, final DAO dao, final Coordinator coordinator) {
        this.tag = tag;
        this.dao = dao;
        this.coordinator = coordinator;
        this.changes = new MemTable(coordinator.getBytesFlushThreshold());
    }

    /**
     * Returns an iterator over the elements in storage and transaction elements
     *
     * @param from the key with which the iteration begins
     * @return iterator
     * @throws IOException if a write error has occurred
     */
    @NotNull
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        assertClosed();
        final MemTable remaining = changes;
        SSTable changesFile;
        try {
            changesFile = new SSTable(coordinator.getFolder(tag));
        } catch (NoSuchFileException e) {
            changesFile = null;
        }
        final List<Iterator<Record>> iterators = new ArrayList<>();
        final Iterator<Record> newIterator = Iterators.transform(dao.iterator(from), i -> {
            ByteBuffer key = i.getKey();
            if (changes.contains(key)) {
                remaining.remove(key);
                try {
                    return Record.of(key, changes.get(key));
                } catch (IOException e) {
                    logger.error(e.getMessage());
                    return null;
                }
            } else {
                return Record.of(i.getKey(), i.getValue());
            }
        });
        iterators.add(Iterators.transform(remaining.iterator(TOMBSTONE), i -> Record.of(i.getKey(), i.getValue())));
        if (changesFile != null) {
            iterators.add(Iterators.transform(changesFile.getIterator(TOMBSTONE), i -> Record.of(i.getKey(), i.getValue())));
        }
        iterators.add(newIterator);
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
     * @param key target key
     * @param value target value
     * @throws IOException if a flush error has occurred
     */
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        assertClosed();
        assertKeyLocked(key);
        coordinator.lockKey(tag, key);
        changes.upsert(key, value);
        if (changes.isFlushNeeded()) {
            changes.flush(coordinator.getFolder(tag));
        }
    }

    /**
     * Removes value by given key.
     *
     * @param key target key
     * @throws IOException if a flush error has occurred
     */
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        assertClosed();
        assertKeyLocked(key);
        coordinator.lockKey(tag, key);
        changes.upsert(key, TOMBSTONE);
        if (changes.isFlushNeeded()) {
            changes.flush(coordinator.getFolder(tag));
        }
    }

    /**
     * Return value by tag if it is exist
     *
     * @param key target key
     * @return found value by tag
     * @throws IOException if can’t get an iterator
     * @throws NoSuchFileException if value not found
     */
    public ByteBuffer get(final ByteBuffer key) throws IOException {
        assertClosed();
        if (!changes.contains(key)) {
            return dao.get(key);
        } else if (changes.get(key).equals(TOMBSTONE)) {
            throw new NoSuchElementException();
        }

        return changes.get(key);
    }

    /**
     * Applies transaction changes
     */
    public void commit() {
        changes.iterator(TOMBSTONE).forEachRemaining((item) -> {
            try {
                if (item.getValue().equals(TOMBSTONE)) {
                    dao.remove(item.getKey());
                } else {
                    dao.upsert(item.getKey(), item.getValue());
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        });
        close();
    }

    /**
     * Cancels transaction changes
     */
    public void abort() {
        close();
    }

    /**
     * Returns transaction tag
     *
     * @return transaction tag
     */
    public String getTag() {
        return tag;
    }

    @Override
    public void close() {
        changes.iterator(TOMBSTONE).forEachRemaining((item) -> coordinator.unlockKey(item.getKey()));
        try {
            Files.deleteIfExists(coordinator.getFolder(tag).toPath());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        isClosed = true;
    }

    private void assertKeyLocked(final ByteBuffer key) {
        if (coordinator.isLockedByTag(tag, key)) {
            throw new ConcurrentModificationException("This key is already in use");
        }
    }

    private void assertClosed() {
        if (isClosed) {
            throw new IllegalStateException("This transaction is already closed, you need to open a new one in order to perform operations");
        }
    }
}
