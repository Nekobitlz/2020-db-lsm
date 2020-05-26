package ru.mail.polis.nekobitlz;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;

import static ru.mail.polis.nekobitlz.Item.getCurrentNanoTime;

public final class MemTable {

    private final SortedMap<ByteBuffer, Item> data = new TreeMap<>();
    private final long bytesFlushThreshold;
    private long bytesSize;

    public MemTable(final long bytesFlushThreshold) {
        this.bytesFlushThreshold = bytesFlushThreshold;
    }

    /**
     * Returns an iterator over the elements in this table.
     *
     * @param from the key with which the iteration begins
     * @return iterator
     */
    @NotNull
    public Iterator<Item> iterator(@NotNull final ByteBuffer from) {
        return data.tailMap(from)
                .values()
                .iterator();
    }

    /**
     * Inserts or updates an existing value in a table.
     *
     * @param key   the key by which to insert the value
     * @param value value to be inserted
     */
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final Item item = new Item(key.duplicate(), value.duplicate(), getCurrentNanoTime());
        calculateBytesSize(data.put(key, item), item);
    }

    /**
     * Removes the value for this key from this table, if it exists.
     *
     * @param key the key by which to remove the value
     */
    public void remove(@NotNull final ByteBuffer key) {
        final Item item = new Item(key.duplicate(), ByteBuffer.allocate(0), -getCurrentNanoTime());
        calculateBytesSize(data.put(key, item), item);
    }

    /**
     * Moves the current MemTable to a file.
     *
     * @param folder destination directory
     * @return the path of the new SSTable
     * @throws IOException if a write error has occurred
     */
    @NotNull
    public Path flush(final File folder) throws IOException {
        final Path path = SSTableUtils.writeTableToDisk(data.values().iterator(), folder);
        data.clear();
        bytesSize = 0;

        return path;
    }

    @NotNull
    public ByteBuffer get(@NotNull ByteBuffer key) throws IOException, NoSuchElementException {
        final Iterator<Item> iter = iterator(key);
        if (!iter.hasNext()) {
            throw new NoSuchElementException("Not found");
        }

        final Item next = iter.next();
        if (next.getKey().equals(key)) {
            return next.getValue();
        } else {
            throw new NoSuchElementException("Not found");
        }
    }

    public boolean contains(@NotNull ByteBuffer key) {
        try {
            get(key);
            return true;
        } catch (NoSuchElementException | IOException e) {
            return false;
        }
    }

    public boolean isFlushNeeded() {
        return bytesSize > bytesFlushThreshold;
    }

    private void calculateBytesSize(final Item previousItem, final Item item) {
        bytesSize += previousItem == null ? item.getBytesSize() : item.getBytesSize() - previousItem.getBytesSize();
    }
}
