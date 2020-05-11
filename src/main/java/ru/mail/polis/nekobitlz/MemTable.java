package ru.mail.polis.nekobitlz;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import static ru.mail.polis.nekobitlz.Item.getCurrentNanoTime;

public final class MemTable {

    private final SortedMap<ByteBuffer, Item> data = new TreeMap<>();
    private final long bytesFlushThreshold;
    private long bytesSize;

    public MemTable(final long bytesHeapSize) {
        bytesFlushThreshold = bytesHeapSize / 16;
    }

    @NotNull
    public Iterator<Item> iterator(@NotNull final ByteBuffer from) {
        return data.tailMap(from)
                .values()
                .iterator();
    }

    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final Item item = new Item(key.duplicate(), value.duplicate(), getCurrentNanoTime());
        calculateBytesSize(data.put(key, item), item);
    }

    public void remove(@NotNull final ByteBuffer key) {
        final Item item = new Item(key.duplicate(), ByteBuffer.allocate(0), -getCurrentNanoTime());
        calculateBytesSize(data.put(key, item), item);
    }

    @NotNull
    public Path flush(final File folder) throws IOException {
        final Path path = SSTableUtils.writeTableToDisk(data.values().iterator(), folder);
        data.clear();
        bytesSize = 0;

        return path;
    }

    public boolean isFlushNeeded() {
        return bytesSize > bytesFlushThreshold;
    }

    private void calculateBytesSize(final Item previousItem, final Item item) {
        if (previousItem == null) {
            bytesSize += item.getBytesSize();
        } else {
            bytesSize += item.getBytesSize() - previousItem.getBytesSize();
        }
    }
}
