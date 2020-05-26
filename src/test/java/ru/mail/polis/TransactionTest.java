package ru.mail.polis;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.mail.polis.nekobitlz.Transaction;
import ru.mail.polis.nekobitlz.TransactionDAO;
import ru.mail.polis.nekobitlz.TransactionDAOImpl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TransactionTest extends TestBase {

    @Test
    void simpleOperations(@TempDir File data) throws IOException {
        // Reference value
        final int valueSize = 1024 * 1024;
        final int keyCount = 10;

        final ByteBuffer value = randomBuffer(valueSize);
        final Collection<ByteBuffer> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add(randomKey());
        }

        try (TransactionDAO transactionDAO = new TransactionDAOImpl(data, 1024 * 1024 * 16)) {
            DAO dao = transactionDAO.getStorage();

            Transaction transaction = transactionDAO.beginTransaction("test");
            for (final ByteBuffer key : keys) {
                transaction.upsert(key, join(key, value));
            }
            for (final ByteBuffer key : keys) {
                assertEquals(join(key, value), transaction.get(key));
                assertThrows(NoSuchElementException.class, () -> dao.get(key));
            }
            transaction.commit();
            for (final ByteBuffer key : keys) {
                assertEquals(join(key, value), dao.get(key));
            }

            Transaction transaction1 = transactionDAO.beginTransaction("test1");
            for (final ByteBuffer key : keys) {
                transaction1.remove(key);
            }
            for (final ByteBuffer key : keys) {
                assertThrows(NoSuchElementException.class, () -> transaction1.get(key));
                assertEquals(join(key, value), dao.get(key));
            }
            transaction1.commit();
            for (final ByteBuffer key : keys) {
                assertThrows(NoSuchElementException.class, () -> dao.get(key));
            }

            Transaction transaction2 = transactionDAO.beginTransaction("test2");
            for (final ByteBuffer key : keys) {
                transaction2.upsert(key, join(key, value));
            }
            for (final ByteBuffer key : keys) {
                assertEquals(join(key, value), transaction2.get(key));
                assertThrows(NoSuchElementException.class, () -> dao.get(key));
            }
            transaction2.abort();
            for (final ByteBuffer key : keys) {
                assertThrows(NoSuchElementException.class, () -> dao.get(key));
                assertThrows(IllegalStateException.class, () -> transaction2.get(key));
                assertThrows(IllegalStateException.class, () -> transaction2.remove(key));
                assertThrows(IllegalStateException.class, () -> transaction2.upsert(key, join(key, value)));
                assertThrows(IllegalStateException.class, transaction2::commit);
                assertThrows(IllegalStateException.class, transaction2::abort);
            }
        }
    }

    @Test
    void twoOperations(@TempDir File data) throws IOException {
        // Reference value
        final int valueSize = 1024 * 1024;
        final int keyCount = 10;

        final ByteBuffer value = randomBuffer(valueSize);
        final Collection<ByteBuffer> keys = new ArrayList<>(keyCount);
        final Collection<ByteBuffer> keys1 = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add(randomKey());
        }
        for (int i = 0; i < keyCount; i++) {
            keys1.add(randomKey());
        }

        try (TransactionDAO transactionDAO = new TransactionDAOImpl(data, 1024 * 1024 * 16)) {
            DAO dao = transactionDAO.getStorage();
            Transaction transaction = transactionDAO.beginTransaction("test");
            Transaction transaction1 = transactionDAO.beginTransaction("test1");

            for (final ByteBuffer key : keys) {
                transaction.upsert(key, join(key, value));
            }
            for (final ByteBuffer key : keys) {
                assertEquals(join(key, value), transaction.get(key));
                assertThrows(ConcurrentModificationException.class, () -> transaction1.upsert(key, join(key, value)));
            }
            transaction.abort();

            for (final ByteBuffer key : keys) {
                transaction1.upsert(key, join(key, value));
                assertEquals(join(key, value), transaction1.get(key));
            }
            transaction1.commit();

            for (final ByteBuffer key : keys) {
                assertEquals(join(key, value), dao.get(key));
            }

            Transaction transaction2 = transactionDAO.beginTransaction("test2");
            Transaction transaction3 = transactionDAO.beginTransaction("test3");

            for (final ByteBuffer key : keys) {
                transaction2.remove(key);
            }
            for (final ByteBuffer key : keys) {
                assertThrows(NoSuchElementException.class, () -> transaction2.get(key));
                assertThrows(ConcurrentModificationException.class, () -> transaction3.remove(key));
            }
            transaction2.abort();

            for (final ByteBuffer key : keys) {
                transaction3.remove(key);
                assertThrows(NoSuchElementException.class, () -> transaction3.get(key));
            }
            transaction3.commit();

            for (final ByteBuffer key : keys) {
                assertThrows(NoSuchElementException.class, () -> dao.get(key));
            }
        }
    }
}
