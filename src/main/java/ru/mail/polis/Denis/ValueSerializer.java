package ru.mail.polis.Denis;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

class ValueSerializer {
    public static final ValueSerializer INSTANCE = new ValueSerializer();

    public byte[] serialize(@NotNull Value value) {
        int length = 12 + value.getData().length;
        ByteBuffer buffer = ByteBuffer.allocate(length);
        Stream.of(buffer)
                .map(e -> e.putLong(value.getTimestamp())).map(e -> e.putInt(value.getState().ordinal()))
                .map(e -> e.put(value.getData())).toArray();
        return buffer.array();
    }

    public Value deserialize(byte[] serializedValue) {
        ByteBuffer buffer = ByteBuffer.wrap(serializedValue);
        long timestamp = buffer.getLong();
        int state = buffer.getInt();
        byte[] value = new byte[serializedValue.length - 12];
        buffer.get(value);
        return new Value(value, timestamp, Value.stateCode.values()[state]);
    }
}
