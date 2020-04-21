package ru.mail.polis.nekobitlz;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.TreeMap;

public class DAOImpl implements DAO {

    private final TreeMap<ByteBuffer, ByteBuffer> data = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        return data
                .tailMap(from)
                .entrySet()
                .stream()
                .map(it -> Record.of(it.getKey(), it.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        data.put(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        data.remove(key);
    }

    @Override
    public void close() {
        data.clear();
    }
}
