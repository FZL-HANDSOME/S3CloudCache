package org.foreverfzl.cloudchache.common.cache;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * 一个默认5MB的缓冲块
 */
public class CloudCacheBlock implements UpLoad {
    private long blockId;
    private String s3Key;

    // 仅仅记录该 Block 在全局 连续堆外内存中的“物理起跑线”
    private final long blockBaseOffset;
    private final int blockSize;

    protected static final AtomicIntegerFieldUpdater<CloudCacheBlock> WROTE_POSITION_UPDATER;
    protected static final AtomicIntegerFieldUpdater<CloudCacheBlock> REFERENCE_COUNT;

    private volatile int writePosition; //写指针
    private volatile int referenceCount; //目前有多少个线程正在向该Block中写入
    private volatile boolean available = true; //该block是否可写入

    private volatile long walMinPosition; //该Block中的数据目前对应WAL文件的最小位置
    private volatile long walMaxPosition; //该Block中的数据目前对应WAL文件的最大位置

    static {
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(CloudCacheBlock.class, "writePosition");
        REFERENCE_COUNT = AtomicIntegerFieldUpdater.newUpdater(CloudCacheBlock.class, "referenceCount");
    }

    public CloudCacheBlock(long blockBaseOffset, int blockSize, long blockId) {
        this.blockBaseOffset = blockBaseOffset;
        this.blockSize = blockSize;
        this.blockId = blockId;
    }

    public long getBlockId() {
        return blockId;
    }

    public String getS3Key() {
        return s3Key;
    }

    public long getBlockBaseOffset() {
        return blockBaseOffset;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getWritePosition() {
        return writePosition;
    }

    public int getReferenceCount() {
        return referenceCount;
    }

    public boolean isAvailable() {
        return available;
    }

    public long getWalMinPosition() {
        return walMinPosition;
    }

    public long getWalMaxPosition() {
        return walMaxPosition;
    }

    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }

    public void setWritePosition(int writePosition) {
        this.writePosition = writePosition;
    }

    public void setReferenceCount(int referenceCount) {
        this.referenceCount = referenceCount;
    }

    public void setWalMaxPosition(long walMaxPosition) {
        this.walMaxPosition = walMaxPosition;
    }

    public void setWalMinPosition(long walMinPosition) {
        this.walMinPosition = walMinPosition;
    }

    public void setBlockId(long blockId) {
        this.blockId = blockId;
    }
}
