package ru.mail.polis.nekobitlz;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.*;
import java.util.*;

import static java.nio.file.StandardOpenOption.*;

public class SSTable {

    private final ByteBuffer records;
    private final LongBuffer offsets;
    private final File file;
    private final Long recordCount;

    public SSTable(@NotNull File file) throws IOException {
        this.file = file;
        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(file.toPath(), READ)) {
            assertCondition(fileChannel.size() >= Long.BYTES);

            final ByteBuffer mappedByteBuffer = fileChannel
                    .map(MapMode.READ_ONLY, 0, file.length())
                    .order(ByteOrder.BIG_ENDIAN);
            assertCondition(mappedByteBuffer.limit() < Integer.MAX_VALUE);

            recordCount = mappedByteBuffer.getLong(mappedByteBuffer.limit() - Long.BYTES);
            assertCondition(mappedByteBuffer.limit() > recordCount * 21);

            offsets = mappedByteBuffer
                    .duplicate()
                    .position(getRemaining(mappedByteBuffer.limit()))
                    .limit(mappedByteBuffer.limit() - Long.BYTES)
                    .slice()
                    .asLongBuffer();
            assertCondition(offsets.limit() == recordCount);

            records = mappedByteBuffer
                    .duplicate()
                    .limit(getRemaining(mappedByteBuffer.limit()))
                    .slice()
                    .asReadOnlyBuffer();
        }
    }

    public File getFile() {
        return file;
    }

    public Iterator<Item> getIterator(final ByteBuffer from) {
        return new Iterator<>() {
            long pos = getPosition(from);

            @Override
            public boolean hasNext() {
                return pos < recordCount;
            }

            @Override
            public Item next() {
                final Item item = getItem(pos);
                pos++;
                return item;
            }
        };
    }

    private int getRemaining(int limit) {
        return (int) (limit - Long.BYTES * (recordCount + 1));
    }

    @NotNull
    private Item getItem(final long pos) {
        final ByteBuffer record = getRecord(pos);
        final ByteBuffer key = getKey(record);
        final long timeStamp = getTimeStamp(record);
        ByteBuffer value = timeStamp >= 0 ? getValue(record) : ByteBuffer.allocate(0);

        return new Item(key.duplicate(), value.duplicate(), timeStamp);
    }

    private ByteBuffer getRecord(final long index) {
        final int intIndex = (int) index;
        final long offset = offsets.get(intIndex);
        long recordLimit = recordCount - index == 1 ? records.limit() : offsets.get(intIndex + 1);

        return records.duplicate()
                .position((int) offset)
                .limit((int) recordLimit)
                .slice()
                .asReadOnlyBuffer();
    }

    private ByteBuffer getKey(@NotNull final ByteBuffer record) {
        final ByteBuffer rec = record.duplicate();
        final int keySize = rec.getInt();
        return rec.limit(Integer.BYTES + keySize)
                .slice()
                .asReadOnlyBuffer();
    }

    private ByteBuffer getValue(@NotNull final ByteBuffer record) {
        final ByteBuffer rec = record.duplicate();
        final int keySize = rec.getInt();
        return rec.position(Integer.BYTES + keySize + Long.BYTES * 2)
                .slice()
                .asReadOnlyBuffer();
    }

    private long getTimeStamp(@NotNull final ByteBuffer record) {
        final ByteBuffer rec = record.duplicate();
        return rec.position(Integer.BYTES + rec.getInt())
                .getLong();
    }

    private long getPosition(final ByteBuffer key) {
        long left = 0;
        long right = recordCount - 1;
        while (left <= right) {
            final long mid = left + (right - left) / 2;
            final int compare = getKey(getRecord(mid)).compareTo(key);
            if (compare > 0) {
                right = mid - 1;
            } else if (compare < 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    private void assertCondition(boolean b) {
        if (!b) {
            throw new IllegalArgumentException();
        }
    }
}
