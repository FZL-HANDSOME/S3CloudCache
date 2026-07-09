package org.foreverfzl.cloudcache.core.cache;

import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * 一个默认8MB的缓冲块
 */
public class CloudCacheBlock extends CacheBlockReferenceResource implements CacheBlock {
    //获取一个干净的block的时候根据用户传进来的字符串 生成一个唯一的s3Key
    private String s3Key;

    // 仅仅记录该 Block 在全局 连续堆外内存中的“物理起跑线”
    private final long blockFromOffset;
    private final int blockSize;

    protected static final AtomicLongFieldUpdater<CloudCacheBlock> WROTE_POSITION_UPDATER;
    private volatile long writePosition; //写指针

    //globalMemorySegment的一个切片
    protected final MemorySegment memorySegment;
    private final CacheBlockManager manager;

    //逻辑位点层，这一部分在分配WAL文件写指针后确认
    private String fileName;
    private int logicalIndex;  // 它在这个 WAL 文件内部的逻辑序号（0, 1, 2...）

    static {
        WROTE_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(CloudCacheBlock.class, "writePosition");
    }

    public CloudCacheBlock(long blockFromOffset, int blockSize, MemorySegment memorySegment, CacheBlockManager manager) {
        this.blockFromOffset = blockFromOffset;
        this.blockSize = blockSize;
        this.memorySegment = memorySegment;
        this.manager = manager;
    }

    /**
     * 原子抢占当前Block写入空间
     *
     * @param size 本次需要写入的数据大小
     * @return 抢到的起始写位置，如果空间不足返回 -1
     */
    public long tryAcquireWritePosition(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("size must be greater than 0");
        }
        long currentPosition;
        long nextPosition;
        do {
            currentPosition = WROTE_POSITION_UPDATER.get(this);
            nextPosition = currentPosition + size;
            // Block空间不足
            if (nextPosition > blockSize) {
                return -1;
            }
        } while (!WROTE_POSITION_UPDATER.compareAndSet(this, currentPosition, nextPosition));
        return currentPosition;
    }

    //获取写指定区切片
    public MemorySegment getWriteMemorySegment(long fromOffset, long dataLen) {
        return memorySegment.asSlice(fromOffset, dataLen);
    }

    //获取上传指定分片区域
    public MemorySegment getUpdateMemorySegment() {
        return memorySegment.asSlice(0, writePosition);
    }


    /**
     * 业务线程完成写入后的收尾逻辑，修改Block的各个信息
     */
    public void releaseReference() {
        // 1. 递减当前正在写入的线程数
        long refs = this.refCount.decrementAndGet();
        //最后一个线程看是否满足上传需求
        if (refs == 0) {
            if (manager.blockMetaDataManager.canUpload(this.fileName, this.logicalIndex)) {
                manager.updateBlock(this);
            }
        }
    }

    @Override
    public void getReference() {
        this.refCount.incrementAndGet();
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

    public int getLogicalIndex() {
        return logicalIndex;
    }

    public void setLogicalIndex(int logicalIndex) {
        this.logicalIndex = logicalIndex;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void clean() {
        this.s3Key = null;
        this.writePosition = 0;
        this.fileName = null;
        this.logicalIndex = 0;
        this.refCount.set(0);
    }
}
