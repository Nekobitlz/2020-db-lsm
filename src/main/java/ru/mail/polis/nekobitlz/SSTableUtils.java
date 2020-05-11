package ru.mail.polis.nekobitlz;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public class SSTableUtils {

    public static final String VALID_FILE_EXTENSION = ".dat";
    private static final String TEMP_FILE_EXTENSION = ".temp";

    public static boolean hasValidFileExtension(@NotNull Path path) {
        return path.getFileName()
                .toString()
                .endsWith(VALID_FILE_EXTENSION);
    }

    @NotNull
    public static Path writeTableToDisk(@NotNull final Iterator<Item> items, @NotNull final File folder) throws IOException {
        final List<Long> offsets = new ArrayList<>();
        final String uuid = UUID.randomUUID().toString();

        final String fileName = uuid + TEMP_FILE_EXTENSION;
        final String fileNameComplete = uuid + VALID_FILE_EXTENSION;

        final Path folderPath = folder.toPath();
        final Path path = folderPath.resolve(Paths.get(fileName));
        final Path pathComplete = folderPath.resolve(Paths.get(fileNameComplete));

        long offset = 0;
        offsets.add(offset);

        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(path, WRITE, CREATE)) {
            while (items.hasNext()) {
                final Item currentItem = items.next();
                writeItemToFile(fileChannel, currentItem);
                offset += currentItem.getBytesSize();
                offsets.add(offset);
            }
            writeOffsetToFile(offsets, fileChannel);
            Files.move(path, pathComplete, StandardCopyOption.ATOMIC_MOVE);
        }

        return pathComplete;
    }

    private static void writeItemToFile(FileChannel fileChannel, @NotNull Item currentItem) throws IOException {
        final ByteBuffer key = currentItem.getKey();
        final ByteBuffer value = currentItem.getValue();
        final ByteBuffer row = ByteBuffer.allocate((int) currentItem.getBytesSize());

        row.putInt(key.remaining())
                .put(key.duplicate())
                .putLong(currentItem.getTimeStamp());

        if (!currentItem.isRemoved()) {
            row.putLong(value.remaining()).put(value.duplicate());
        }

        row.flip();
        fileChannel.write(row);
    }

    private static void writeOffsetToFile(@NotNull List<Long> offsets, FileChannel fileChannel) throws IOException {
        final int offsetsCount = offsets.size();
        final ByteBuffer offsetsByteBuffer = ByteBuffer.allocate(offsetsCount * Long.BYTES);
        offsets.set(offsetsCount - 1, (long) offsetsCount - 1);

        for (final Long i : offsets) {
            offsetsByteBuffer.putLong(i);
        }

        offsetsByteBuffer.flip();
        fileChannel.write(offsetsByteBuffer);
    }
}
