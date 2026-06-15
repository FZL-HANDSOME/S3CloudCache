package org.foreverfzl.cloudcache.core.cache;

import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * 一个默认5MB的缓冲块
 */
public class CloudCacheBlock extends CacheBlockReferenceResource implements CacheBlock {
    private long blockId;
    private String s3Key;

    // 仅仅记录该 Block 在全局 连续堆外内存中的“物理起跑线”
    private final long blockFromOffset;
    private final int blockSize;

    protected static final AtomicLongFieldUpdater<CloudCacheBlock> WROTE_POSITION_UPDATER;
    private volatile long writePosition; //写指针

    //逻辑位点层，这一部分在分配WAL文件写指针后确认的
    //为了保证每个Block里面只能存放一个WAL文件的数据，如果为null说明该Block里面没有数据，否则代表该Block存放了对应文件的数据
    private DefaultMappedFile mappedFile;
    private volatile int logicalIndex;  // 它在这个 WAL 文件内部的逻辑序号（0, 1, 2...）
    private volatile long endOffsetInMappedFile; //由最后一个写入线程修改

    static {
        WROTE_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(CloudCacheBlock.class, "writePosition");
    }

    public CloudCacheBlock(long blockFromOffset, int blockSize, long blockId) {
        this.blockFromOffset = blockFromOffset;
        this.blockSize = blockSize;
        this.blockId = blockId;
    }

    /**
     * 业务线程完成写入后的收尾逻辑，修改Block的各个信息
     */
    public void releaseReference() {
        // 1. 递减当前正在写入的线程数
        long refs = this.refCount.decrementAndGet();
        // 2. 如果自己是最后一个离开的，且该 Block 已经宣布封板（不再接收新数据）
        if (refs == 0 && !this.available) {
            // 🚨 终极计算：由于是顺序写入，这个 Block 最终写到了哪里，writePosition 最清楚！
            // 逻辑起始位点 = 组装时的 index * 块大小
            long blockBaseOffsetInFile = (long) this.logicalIndex * this.blockSize;
            // 最终在 MappedFile 中的绝对结束位点 = 起始位点 + 堆外内存实际写入的物理长度
            this.endOffsetInMappedFile = blockBaseOffsetInFile + this.writePosition;
        }
    }

    @Override
    public int getReference() {
        return this.refCount.incrementAndGet();
    }

    public long getBlockId() {
        return blockId;
    }

    public String getS3Key() {
        return s3Key;
    }

    public long getBlockFromOffset() {
        return blockFromOffset;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public long getWritePosition() {
        return writePosition;
    }

    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }

    public void setWritePosition(int writePosition) {
        this.writePosition = writePosition;
    }

    public void setBlockId(long blockId) {
        this.blockId = blockId;
    }
}
