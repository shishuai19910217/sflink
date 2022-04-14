package com.sya.utils;

public class Snowflake {
    public static final int NODE_SHIFT = 10;
    public static final int SEQ_SHIFT = 12;
    public static final short MAX_NODE = 1024;
    public static final short MAX_SEQUENCE = 4096;
    private short sequence;
    private long referenceTime;
    private int node;

    public Snowflake(int node) {
        if (node >= 0 && node <= MAX_NODE) {
            this.node = node;
        } else {
            throw new IllegalArgumentException(String.format("node must be between %s and %s", 0, MAX_NODE));
        }
    }

    public synchronized long next() {
        long currentTime = System.currentTimeMillis();
        long counter;
        if (currentTime < this.referenceTime) {
            throw new RuntimeException(String.format("Last referenceTime %s is after reference time %s", this.referenceTime, currentTime));
        }

        if (currentTime > this.referenceTime) {
            this.sequence = 0;
        } else {
            if (this.sequence >= MAX_SEQUENCE) {
                throw new RuntimeException("Sequence exhausted at " + this.sequence);
            }
            ++this.sequence;
        }

        counter = this.sequence;
        this.referenceTime = currentTime;

        return currentTime << NODE_SHIFT << SEQ_SHIFT | (long) (this.node << SEQ_SHIFT) | counter;
    }
}
