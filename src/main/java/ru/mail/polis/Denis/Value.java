package ru.mail.polis.Denis;

import java.io.Serializable;

public class Value implements Serializable {
    private final byte[] data;
    private final long timestamp;
    private final stateCode state;
    public static final Value UNKNOWN = new Value(new byte[0], 0, stateCode.UNKNOWN);

    enum stateCode {
        PRESENT, DELETED, UNKNOWN
    }

    Value() {
        this.data = new byte[0];
        this.timestamp = System.currentTimeMillis();
        this.state = stateCode.DELETED;
    }

    Value(byte[] value) {
        this.data = value;
        this.timestamp = System.currentTimeMillis();
        state = stateCode.PRESENT;
    }

    Value(byte[] value, long timestamp, stateCode state) {
        this.data = value;
        this.timestamp = timestamp;
        this.state = state;
    }

    public byte[] getData() {
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public stateCode getState() {
        return state;
    }
}

