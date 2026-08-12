package org.foreverfzl.cloudcache.metadata.entity;


import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class BlockMetaData {

    //0代表开放，1代表封口，2代表上传中，3代表上传成功，4代表上传失败
    //           OPEN(0)
    //              │
    //              ▼
    //         SEALED(1)
    //              │
    //              ▼
    //      UPLOADING(2)
    //         │     │  ▲
    //         ▼     ▼  │
    //SUCCESS(3)   FAILED(4)
    public static final int OPEN = 0;
    public static final int SEALED = 1;
    public static final int UPLOADING = 2;
    public static final int SUCCESS = 3;
    public static final int FAILED = 4;

    private static final AtomicIntegerFieldUpdater<BlockMetaData> STATE_UPDATER;
    private volatile int state;
    //expectedBytes代表该逻辑block期望的字节数
    private volatile int expectedBytes;
    private static final AtomicIntegerFieldUpdater<BlockMetaData> EXPECTED_BYTES_UPDATER;
    //pageCacheBytes代表该逻辑block写入到操作系统PageCache的字节数
    private volatile int pageCacheBytes;
    private static final AtomicIntegerFieldUpdater<BlockMetaData> PAGE_CACHE_BYTES_UPDATER;
    //finishedBytes代表物理Block真正写入字节数
    private volatile int finishedBytes;
    private static final AtomicIntegerFieldUpdater<BlockMetaData> FINISHED_BYTES_UPDATER;
    private volatile long lastActiveTime;
    //该Broken指的是 物理Block，如果写入数据出错则为true
    private volatile boolean isBroken;


    static {
        STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "state");
        EXPECTED_BYTES_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "expectedBytes");
        PAGE_CACHE_BYTES_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "pageCacheBytes");
        FINISHED_BYTES_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "finishedBytes");
    }

    public BlockMetaData() {
        this.state = 0;
        this.expectedBytes = 0;
        this.finishedBytes = 0;
        this.lastActiveTime = System.currentTimeMillis();
    }


    //尝试将Block设置为封口
    public boolean trySeal() {
        return STATE_UPDATER.compareAndSet(this, OPEN, SEALED);
    }

    //尝试将Block设置为上传
    public boolean tryStartUpload() {
        return STATE_UPDATER.compareAndSet(this, SEALED, UPLOADING);
    }

    //尝试将Block设置上传成功
    public boolean markUploadSuccess() {
        return STATE_UPDATER.compareAndSet(this, UPLOADING, SUCCESS);
    }

    //尝试将Block设置为上传失败
    public boolean markUploadFailed() {
        return STATE_UPDATER.compareAndSet(this, UPLOADING, FAILED);
    }


    //尝试将Block设置为上传中，一般重试上传的时候会用
    public boolean retryUpload() {
        return STATE_UPDATER.compareAndSet(this, FAILED, UPLOADING);
    }


    public void addPageCacheBytes(int val) {
        int current;
        int next;
        do {
            current = PAGE_CACHE_BYTES_UPDATER.get(this);
            next = current + val;
        } while (!PAGE_CACHE_BYTES_UPDATER.compareAndSet(this, current, next));
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

    public void updateLastTime() {
        this.lastActiveTime = System.currentTimeMillis(); //这里可以容纳误差，简单赋值即可
    }

    public int getPageCacheBytes() {
        return pageCacheBytes;
    }

    public int getFinishedBytes() {
        return finishedBytes;
    }

    public int getState() {
        return state;
    }

    public int getExpectedBytes() {
        return expectedBytes;
    }

    public long getLastActiveTime() {
        return lastActiveTime;
    }

    public boolean isBroken() {
        return isBroken;
    }

    public void setBroken() {
        isBroken = true;
    }
}
