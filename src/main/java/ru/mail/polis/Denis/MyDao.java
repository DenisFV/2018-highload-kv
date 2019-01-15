package ru.mail.polis.Denis;

import org.jetbrains.annotations.NotNull;
import org.mapdb.*;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Stream;

public class MyDao implements KVDao {

    private final DB db;
    private final Map<byte[], byte[]> map;

    public MyDao(File data) {
        this.db = DBMaker.fileDB(data.getAbsolutePath() + "//db").make();
        this.map = db.treeMap("data", Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY).createOrOpen();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        return Stream.of(map.get(key))
                .filter(Objects::nonNull).findFirst()
                .orElseThrow(NoSuchElementException::new);
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        map.remove(key);
    }

    @Override
    public void close() throws IOException {
        db.close();
    }
}
