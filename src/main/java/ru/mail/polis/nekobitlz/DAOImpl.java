package ru.mail.polis.nekobitlz;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class DAOImpl implements DAO {

    private static final int COMPACTION_THRESHOLD = 16;

    private final MemTable memTable;
    private final File folder;
    private final List<SSTable> tables;
    private final Logger logger = LoggerFactory.getLogger(DAOImpl.class);

    public DAOImpl(@NotNull final File folder, final long bytesHeapSize) throws IOException {
        memTable = new MemTable(bytesHeapSize);
        tables = new ArrayList<>();
        this.folder = folder;

        try (Stream<Path> files = Files.list(folder.toPath())) {
            files.filter(path -> Files.isRegularFile(path) && SSTableUtils.hasValidFileExtension(path))
                    .forEach(path -> createNewSSTable(path.toFile()));
        }

        compactIfNeeded();
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final Iterator<Item> itemIterator = createItemIterator(from);
        return Iterators.transform(itemIterator, i -> Record.of(i.getKey(), i.getValue()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.isFlushNeeded()) {
            flushTable();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.isFlushNeeded()) {
            flushTable();
        }
    }

    @Override
    public void close() throws IOException {
        memTable.flush(folder);
    }

    private void createNewSSTable(final File file) {
        try {
            final SSTable table = new SSTable(file);
            tables.add(table);
        } catch (IOException e) {
            logger.error("Failed to create SSTable from " + file.getName() + e.getMessage());
        } catch (IllegalArgumentException e) {
            logger.error("File skipped: " + file.getName());
        }
    }

    private void flushTable() throws IOException {
        final Path flushedFolder = memTable.flush(folder);
        createNewSSTable(flushedFolder.toFile());
        compactIfNeeded();
    }

    private void compactIfNeeded() throws IOException {
        if (tables.size() > COMPACTION_THRESHOLD) {
            final Iterator<Item> itemIterator = createItemIterator(ByteBuffer.allocate(0));
            final Path mergedTable = SSTableUtils.writeTableToDisk(itemIterator, folder);
            tables.forEach(table -> {
                try {
                    Files.delete(table.getFile().toPath());
                } catch (IOException e) {
                    logger.error("Failed to delete file " + table.getFile().getName() + e.getMessage());
                }
            });
            tables.clear();
            createNewSSTable(mergedTable.toFile());
        }
    }

    @NotNull
    private Iterator<Item> createItemIterator(@NotNull final ByteBuffer from) {
        final List<Iterator<Item>> iterators = new ArrayList<>();
        iterators.add(memTable.iterator(from));

        for (final SSTable table : tables) {
            iterators.add(table.getIterator(from));
        }

        final Iterator<Item> mergedIterator = Iterators.mergeSorted(iterators, Item.COMPARATOR);
        final Iterator<Item> collapsedIterator = Iters.collapseEquals(mergedIterator, Item::getKey);

        return Iterators.filter(collapsedIterator, i -> !i.isRemoved());
    }
}
