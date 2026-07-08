package org.foreverfzl.cloudcache.metadata.entity;


import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class BlockMetaData {

    //0代表开放，1代表封口上传
    private volatile int state;
    private static final AtomicIntegerFieldUpdater<BlockMetaData> EXPECTED_BYTES_UPDATER;
    private volatile int expectedBytes;
    private static final AtomicIntegerFieldUpdater<BlockMetaData> FINISHED_BYTES_UPDATER;
    private volatile int finishedBytes;

    static {
        EXPECTED_BYTES_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "expectedBytes");
        FINISHED_BYTES_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "finishedBytes");
    }

    public BlockMetaData() {
        this.state = 0;
        this.expectedBytes = 0;
        this.finishedBytes = 0;
    }

    public void addExpectedBytes(int val) {
        int current;
        int next;
        do {
            current = EXPECTED_BYTES_UPDATER.get(this);
            next = current + val;
        } while (!EXPECTED_BYTES_UPDATER.compareAndSet(this, current, next));
    }

    public void addFinishedBytes(int val) {
        int current;
        int next;
        do {
            current = FINISHED_BYTES_UPDATER.get(this);
            next = current + val;
        } while (!FINISHED_BYTES_UPDATER.compareAndSet(this, current, next));
    }

    public void setState(int val) {
        this.state = val;
    }

    public int getState() {
        return state;
    }

    public int getExpectedBytes() {
        return expectedBytes;
    }

    public int getFinishedBytes() {
        return finishedBytes;
    }
}
