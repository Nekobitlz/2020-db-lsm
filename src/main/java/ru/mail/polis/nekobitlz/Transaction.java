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
import java.util.*;

public class Transaction implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(Transaction.class);

    private final String tag;
    private final Map<ByteBuffer, ByteBuffer> changes = new HashMap<>();
    private final DAO dao;
    private final Coordinator coordinator;
    private boolean isClosed;

    public Transaction(String tag, DAO dao, Coordinator coordinator) {
        this.tag = tag;
        this.dao = dao;
        this.coordinator = coordinator;
    }

    @NotNull
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        assertClosed();
        final Map<ByteBuffer, ByteBuffer> remaining = changes;
        final List<Iterator<Record>> iterators = new ArrayList<>();
        final Iterator<Record> newIterator = Iterators.transform(dao.iterator(from), i -> {
            ByteBuffer key = i.getKey();
            if (changes.containsKey(key)) {
                remaining.remove(key);
                return Record.of(key, changes.get(key));
            } else {
                return Record.of(i.getKey(), i.getValue());
            }
        });
        iterators.add(Iterators.transform(remaining.entrySet().iterator(), i -> Record.of(i.getKey(), i.getValue())));
        iterators.add(newIterator);
        final Iterator<Record> mergedIterator = Iterators.mergeSorted(iterators, Comparator
                .comparing(Record::getKey)
                .thenComparing(Comparator
                        .comparing(Record::getValue)
                        .reversed()
                ));
        final Iterator<Record> collapsedIterator = Iters.collapseEquals(mergedIterator, Record::getKey);

        return Iterators.filter(collapsedIterator, i -> !i.getValue().equals(ByteBuffer.allocate(0)));
    }

    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        assertClosed();
        assertKeyLocked(key);
        coordinator.lockKey(tag, key);
        changes.put(key, value);
    }

    public void remove(@NotNull final ByteBuffer key) {
        assertClosed();
        assertKeyLocked(key);
        coordinator.lockKey(tag, key);
        changes.put(key, ByteBuffer.allocate(0));
    }

    public ByteBuffer get(ByteBuffer key) throws IOException {
        assertClosed();
        if (!changes.containsKey(key)) {
           return dao.get(key);
        } else if (changes.get(key).equals(ByteBuffer.allocate(0))) {
            throw new NoSuchElementException();
        }

        return changes.get(key);
    }

    public void commit() {
        assertClosed();
        changes.forEach((key, value) -> {
            try {
                if (value.equals(ByteBuffer.allocate(0))) {
                    dao.remove(key);
                } else {
                    dao.upsert(key, value);
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        });
        close();
    }

    public String getTag() {
        return tag;
    }

    public void abort() {
        assertClosed();
        close();
    }

    @Override
    public void close() {
        assertClosed();
        changes.forEach((key, value) -> coordinator.unlockKey(key));
        changes.clear();
        coordinator.removeTransaction(this);
        isClosed = true;
    }

    private void assertKeyLocked(ByteBuffer key) {
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
