package org.foreverfzl.cloudcache.core.cache;

import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * 一个默认5MB的缓冲块
 */
public class CloudCacheBlock extends CacheBlockReferenceResource implements CacheBlock {
    //获取一个干净的block的时候根据用户传进来的字符串 生成一个唯一的s3Key
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


    static {
        WROTE_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(CloudCacheBlock.class, "writePosition");
    }

    public CloudCacheBlock(long blockFromOffset, int blockSize, long writePosition) {
        this.blockFromOffset = blockFromOffset;
        this.blockSize = blockSize;
        this.writePosition = writePosition;
    }


    /**
     * 业务线程完成写入后的收尾逻辑，修改Block的各个信息
     */
    public void releaseReference() {
        // 1. 递减当前正在写入的线程数
        long refs = this.refCount.decrementAndGet();
       //todo

    }

    @Override
    public int getReference() {
        return this.refCount.incrementAndGet();
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


    public DefaultMappedFile getMappedFile() {
        return mappedFile;
    }

    public void setMappedFile(DefaultMappedFile mappedFile) {
        this.mappedFile = mappedFile;
    }

    public int getLogicalIndex() {
        return logicalIndex;
    }

    public void setLogicalIndex(int logicalIndex) {
        this.logicalIndex = logicalIndex;
    }

    public void clean() {
        this.s3Key = null;
        this.writePosition = 0;
        this.mappedFile = null;
        this.logicalIndex = 0;
        this.refCount.set(0);
    }
}
