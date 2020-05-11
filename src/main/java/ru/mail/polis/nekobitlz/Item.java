package ru.mail.polis.nekobitlz;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Item {

    public static long additionalTime;
    public static final Comparator<Item> COMPARATOR = Comparator
            .comparing(Item::getKey)
            .thenComparing(Comparator
                    .comparing(Item::getTimeStampAbs)
                    .reversed()
            );

    private final ByteBuffer key;
    private final ByteBuffer value;
    private final long timeStamp;

    public Item(final ByteBuffer key, final ByteBuffer value, final long timeStamp) {
        this.key = key;
        this.value = value;
        this.timeStamp = timeStamp;
    }

    public static long getCurrentNanoTime() {
        return System.nanoTime() + additionalTime++;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public boolean isRemoved() {
        return getTimeStamp() < 0;
    }

    public long getBytesSize() {
        final int valueLength = isRemoved() ? 0 : Long.BYTES;
        return (long) Integer.BYTES + key.remaining() + Long.BYTES + value.remaining() + valueLength;
    }

    public long getTimeStampAbs() {
        return Math.abs(timeStamp);
    }
}
