package org.foreverfzl.cloudcache.core.manager;

import org.foreverfzl.cloudcache.core.cache.AppendDataResult;
import org.foreverfzl.cloudcache.core.cache.CacheBlockReferenceResource;
import org.foreverfzl.cloudcache.core.cache.CloudCacheBlock;
import org.foreverfzl.cloudcache.core.datastruct.BlockDataStruct;
import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.metadata.entity.DeadDataInfo;
import org.foreverfzl.cloudcache.metadata.entity.UploadTask;
import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.FutureContext;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.exception.CoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 用于管理一个Bucket的所有Block，也可以理解为那个默认1GB的堆外缓冲区，也就是Block池
 */
public class CacheBlockManager {

    private static final Logger log = LoggerFactory.getLogger(LogName.CACHE_BLOCK_MANAGER);

    //管理的的堆外内存
    private Arena arena;
    private MemorySegment globalMemorySegment;

    private final int blockCount;
    public final String instanceName;
    public final String bucketName;
    //Bucket级别配置文件
    private final BucketConfig config;
    // 空闲/干净的 CloudCacheBlock 池
    private final BlockingQueue<CloudCacheBlock> freeBlocks;

    // 根据自定义 key 维护的 K-V 映射，key为fileFromOffset+BlockIndex
    private final ConcurrentHashMap<Long, CloudCacheBlock> keyBlockMap;
    //block上传者
    private final CacheBlockUpdater blockUpdater;

    //该bucket对应的Block元数据管理者
    public BlockMetaDataManager blockMetaDataManager;

    //正在上传的数量
    protected AtomicInteger upCount = new AtomicInteger(0);

    //正在写入的数量
    protected AtomicInteger writeCount = new AtomicInteger(0);

    //该线程专门获取BlockUpLoadQueueManager类中BlockUpLoadQueue中的任务
    private volatile boolean active = true;
    private volatile boolean closed;
    private Thread getBlockUpLoadQueueTaskThread;


    public CacheBlockManager(String instanceName, String bucketName, BlockMetaDataManager blockMetaDataManager,
                             S3Client s3Client, BucketConfig config, long cacheSize, int cacheBlockSize,
                             int blockUpLoadCount, boolean isCreateThread) {
        this.config = config;
        this.blockCount = (int) (cacheSize / cacheBlockSize);
        this.instanceName = instanceName;
        this.bucketName = bucketName;
        // 1. 创建 MemorySegment 堆外缓冲区
        this.arena = Arena.ofShared();
        this.globalMemorySegment = arena.allocate(cacheSize);
        this.freeBlocks = new ArrayBlockingQueue<>(blockCount);
        this.keyBlockMap = new ConcurrentHashMap<>();
        this.blockUpdater = new CacheBlockUpdater(this, blockUpLoadCount, s3Client, config.enableHeadCheck);
        this.blockMetaDataManager = blockMetaDataManager;
        // 2. 初始化并维护所有的 CloudCacheBlock
        for (int i = 0; i < blockCount; i++) {
            long offset = (long) i * cacheBlockSize;
            CloudCacheBlock block = new CloudCacheBlock(offset, cacheBlockSize, globalMemorySegment.asSlice(offset, cacheBlockSize), this);
            freeBlocks.add(block);
        }
        if (isCreateThread) {
            this.getBlockUpLoadQueueTaskThread = new Thread(this::getBlockUpLoadQueue);
            this.getBlockUpLoadQueueTaskThread.start();
        }
        log.info("Initialized CacheBlockManager with cacheSize={}, blockSize={}, blockCount={},blockUpLoadMaxCount={}",
                cacheSize, cacheBlockSize, blockCount, blockUpLoadCount);
    }


    private void getBlockUpLoadQueue() {
        while (active) {
            try {
                UploadTask task = blockMetaDataManager.getTaskFromUpLoadQueue();
                long fileFromOffset = task.getFileFromOffset();
                int logicalIndex = task.getLogicalIndex();
                log.info("fileFromOffset={}, logicalIndex={} is consumed from upLoadQueue", fileFromOffset, logicalIndex);
                CloudCacheBlock cacheBlock = keyBlockMap.get(ProjectUtil.buildBlockKey(fileFromOffset, logicalIndex));
                if (cacheBlock == null) {
                    //对应Block已被回收(如重复任务)，跳过该任务继续消费，而不是退出消费线程
                    continue;
                }
                //上传
                this.updateBlock(cacheBlock);
            } catch (InterruptedException e) {
                active = false;
                break;
            }
        }
    }

    public AppendDataResult appendData(BlockDataStruct dataStruct, FutureContext futureContext, boolean isAddFuture) {
        return this.appendData(dataStruct, config.s3KeyPrefix, futureContext, isAddFuture);
    }

    /**
     * 往指定的Block中添加数据
     *
     * @param dataStruct 数据
     * @return 结果
     */

    public AppendDataResult appendData(BlockDataStruct dataStruct, String prefix, FutureContext futureContext, boolean isAddFuture) {
        CloudCacheBlock cacheBlock = null;
        int size = dataStruct.getDataLen();
        DefaultMappedFile defaultMappedFile = dataStruct.getDefaultMappedFile();
        long fileFromOffset = defaultMappedFile.fileFromOffset;
        int blockIndex = dataStruct.getBlockIndex();
        BlockMetaData blockMetaData = blockMetaDataManager.getOrCreate(fileFromOffset, blockIndex);
        boolean referenced = false;
        boolean isError = false;
        boolean failFutures = false;
        writeCount.incrementAndGet();
        try {
            boolean alreadyFailed;
            synchronized (blockMetaData) {
                if (isAddFuture) blockMetaData.addFuture(futureContext);
                alreadyFailed = blockMetaData.getState() == BlockMetaData.FAILED;
                if (blockMetaData.getState() == BlockMetaData.SUCCESS) {
                    throw new IllegalStateException("Cannot append to an already committed block");
                }
            }
            if (alreadyFailed) {
                blockMetaData.failAllFuture();
                return AppendDataResult.fail(null, fileFromOffset, blockIndex);
            }
            while (true) {
                cacheBlock = getBlock(fileFromOffset, blockIndex, prefix, defaultMappedFile);
                synchronized (blockMetaData) {
                    if (cacheBlock.getBlockMetaData() != blockMetaData
                            || getExistingBlock(fileFromOffset, blockIndex) != cacheBlock) continue;
                    if (futureContext != null) {
                        futureContext.setS3Key(cacheBlock.getS3Key());
                        futureContext.setSize(size);
                    }
                    if (!cacheBlock.isActive() || cacheBlock.isDelayClean()
                            || (isAddFuture && (blockMetaData.isBroken() || blockMetaData.isRecovering()))
                            || blockMetaData.getState() >= BlockMetaData.UPLOADING) {
                        return AppendDataResult.fail(cacheBlock.getS3Key(), fileFromOffset, blockIndex);
                    }
                    cacheBlock.getReference();
                    referenced = true;
                    break;
                }
            }
            //每个线程抢到自己的写指针
            long curWritePosition = cacheBlock.tryAcquireWritePosition(size);
            MemorySegment cacheBlockSegment = cacheBlock.getWriteMemorySegment(curWritePosition, size);
            //将数据写入Block
            if (!cacheBlock.isActive()) {
                return AppendDataResult.fail(cacheBlock.getS3Key(), fileFromOffset, blockIndex);
            }
            boolean isSuccess = dataStruct.writeTo(cacheBlockSegment);
            if (!isSuccess) {
                //失败后重试一次
                isSuccess = dataStruct.writeTo(cacheBlockSegment);
            }
            if (isSuccess) {
                synchronized (blockMetaData) {
                    if (futureContext != null) futureContext.setPhysicalOffset(curWritePosition);
                    blockMetaData.addFinishedBytes(size);
                }
            } else {
                throw new CoreException("failed to write data in block");
            }
            return new AppendDataResult(cacheBlock.getS3Key(), curWritePosition, size, true);
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            log.error("block write failed, fileOffset={}, blockIndex={}", fileFromOffset, blockIndex, e);
            isError = true;
            return AppendDataResult.fail(null, fileFromOffset, blockIndex);
        } finally {
            try {
                synchronized (blockMetaData) {
                    if (isError) {
                        CloudCacheBlock bound = getExistingBlock(fileFromOffset, blockIndex);
                        if (bound == null || bound.getBlockMetaData() != blockMetaData) {
                            // An interrupted allocation has no physical block to replay into.
                            // Fail the whole logical task, retaining its unacknowledged WAL.
                            blockMetaDataManager.markUploadFailed(fileFromOffset, blockIndex,
                                    new DeadDataInfo(instanceName, bucketName, fileFromOffset, blockIndex,
                                            ProjectUtil.generateUniqueS3Key(prefix, instanceName, bucketName,
                                                    fileFromOffset, blockIndex)));
                            failFutures = true;
                        } else {
                            blockMetaData.setBroken();
                            bound.setUnActive();
                        }
                    }
                    if (isAddFuture) blockMetaData.addCompletedAppendsBytes(size);
                    if (referenced) {
                        cacheBlock.releaseReference();
                    } else if (blockMetaData.isBroken() && !blockMetaData.isRecovering()) {
                        blockMetaDataManager.setTaskToRecoverQueue(blockMetaData, fileFromOffset, blockIndex);
                    }
                }
            } finally {
                writeCount.decrementAndGet();
            }
            if (failFutures) blockMetaData.failAllFuture();
        }
    }


    /**
     * 上传Block
     */
    public void updateBlock(CloudCacheBlock cacheBlock) {
        blockUpdater.upLoadBlock(cacheBlock);
    }

    /**
     * 关闭所有block的写入
     */
    public void closeAllBlock() {
        freeBlocks.forEach(CacheBlockReferenceResource::setUnActive);
        keyBlockMap.forEach((key, block) -> block.setUnActive());
    }

    /**
     * 上传所有封口的Block
     */
    public void updateAllBlock(long deadLine) {
        for (Map.Entry<Long, CloudCacheBlock> entry : keyBlockMap.entrySet()) {
            // The weakly-consistent iterator may contain a just-recycled entry;
            // upload admission validates the captured metadata and binding atomically.
            updateBlock(entry.getValue());
        }
        while (upCount.get() != 0) {
            try {
                if (System.currentTimeMillis() >= deadLine) {
                    return;
                }
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }


    /**
     * 获取一个可用并且干净的 CloudCacheBlock，并将其与指定的 cacheBlockKey 绑定。
     * 如果该 cacheBlockKey 已经关联了某个 Block，则直接返回已有的 Block。
     */
    public CloudCacheBlock getBlock(long fileFromOffset, int blockIndex, String prefix, DefaultMappedFile defaultMappedFile) throws InterruptedException {
        long cacheBlockKey = ProjectUtil.buildBlockKey(fileFromOffset, blockIndex);
        BlockMetaData metadata = blockMetaDataManager.getOrCreate(fileFromOffset, blockIndex);
        while (!closed) {
            CloudCacheBlock existingBlock = keyBlockMap.get(cacheBlockKey);
            if (existingBlock != null) return existingBlock;
            // Never wait for pool capacity while holding a logical-block/stripe lock.
            CloudCacheBlock candidate = freeBlocks.poll(100, TimeUnit.MILLISECONDS);
            if (candidate == null) continue;
            synchronized (metadata) {
                existingBlock = keyBlockMap.get(cacheBlockKey);
                if (existingBlock != null || closed || metadata.getState() >= BlockMetaData.UPLOADING) {
                    if (!freeBlocks.offer(candidate)) throw new IllegalStateException("Duplicate free block");
                    if (existingBlock != null) return existingBlock;
                    if (!closed) throw new IllegalStateException("Cannot allocate a block for a committed/failed task");
                    break;
                }
                candidate.setActive();
                candidate.setBlockMetaData(metadata);
                candidate.setS3Key(ProjectUtil.generateUniqueS3Key(prefix, this.instanceName, this.bucketName, fileFromOffset, blockIndex));
                candidate.setFileFromOffset(fileFromOffset);
                candidate.setLogicalIndex(blockIndex);
                candidate.setDefaultMappedFile(defaultMappedFile);
                keyBlockMap.put(cacheBlockKey, candidate);
                return candidate;
            }
        }
        throw new IllegalStateException("CacheBlockManager is closed");
    }


    //该方法只有出现异常的时候才回去调用
    public CloudCacheBlock getBlock(long fileFromOffset, int blockIndex) throws InterruptedException {
        return getBlock(fileFromOffset, blockIndex, config.s3KeyPrefix, null);
    }


    public CloudCacheBlock getExistingBlock(long fileFromOffset, int blockIndex) {
        return keyBlockMap.get(ProjectUtil.buildBlockKey(fileFromOffset, blockIndex));
    }

    /**
     * 清除block并且放回到空闲池中
     *
     * @param block
     */
    public void cleanAndRecycle(CloudCacheBlock block) {
        if (block == null) return;
        BlockMetaData metadata = block.getBlockMetaData();
        if (metadata == null) return;
        synchronized (metadata) {
            if (block.getBlockMetaData() != metadata) return;
            block.setUnActive();
            block.setDelayClean();
            cleanAndRecycleWithLock(block);
        }
    }

    public void cleanAndRecycleWithLock(CloudCacheBlock block) {
        if (block == null) return;
        BlockMetaData metadata = block.getBlockMetaData();
        if (metadata == null) return;
        synchronized (metadata) {
            if (block.getBlockMetaData() != metadata || !block.isDelayClean() || block.getReferenceCount() != 0) return;
            long key = ProjectUtil.buildBlockKey(block.getFileFromOffset(), block.getLogicalIndex());
            if (!keyBlockMap.remove(key, block)) return;
            block.clean();
            if (!freeBlocks.offer(block)) throw new IllegalStateException("Duplicate block recycling");
        }
    }

    /** Acquire exclusive recovery ownership after all original WAL-to-core appends have settled. */
    public CloudCacheBlock beginRecovery(long fileFromOffset, int blockIndex) {
        BlockMetaData metadata = blockMetaDataManager.getBlockMetaData(fileFromOffset, blockIndex);
        if (metadata == null) return null;
        synchronized (metadata) {
            CloudCacheBlock block = getExistingBlock(fileFromOffset, blockIndex);
            if (block == null || block.getBlockMetaData() != metadata || block.isDelayClean()
                    || !metadata.isBroken() || metadata.isRecovering() || metadata.getState() != BlockMetaData.SEALED
                    || metadata.getExpectedBytes() != metadata.getPageCacheBytes()
                    || metadata.getExpectedBytes() != metadata.getCompletedAppendsBytes()
                    || block.getReferenceCount() != 0) return null;
            metadata.setRecovering(true);
            metadata.clearFinishedBytes();
            block.resetWritePosition();
            block.setActive();
            block.getReference();
            return block;
        }
    }

    public void finishRecovery(CloudCacheBlock block, boolean success) {
        BlockMetaData metadata = block.getBlockMetaData();
        synchronized (metadata) {
            if (!metadata.isRecovering()) throw new IllegalStateException("No recovery lease");
            success = success && metadata.getExpectedBytes() == metadata.getFinishedBytes()
                    && metadata.getPageCacheBytes() == metadata.getFinishedBytes();
            metadata.setRecovering(false);
            if (success) {
                metadata.setUnBroken();
            } else {
                long fileOffset = block.getFileFromOffset();
                int index = block.getLogicalIndex();
                // The original WAL remains unacknowledged for manual/restart recovery.
                blockMetaDataManager.markUploadFailed(fileOffset, index,
                        new DeadDataInfo(instanceName, bucketName, fileOffset, index, block.getS3Key()));
                block.setUnActive();
                block.setDelayClean();
            }
            block.releaseReference();
        }
        if (!success) metadata.failAllFuture();
    }

    public void waitWriterFinished(long deadline) {
        //1：等待所有的线程写入完成
        while (writeCount.get() != 0) {
            try {
                if (System.currentTimeMillis() >= deadline) {
                    //超时退出
                    log.warn("{} close timeout, force shutdown", bucketName);
                    return;
                }
                Thread.sleep(500);
            } catch (Exception e) {
            }
        }
    }

    public void stopAllThread() {
        active = false;
        if (getBlockUpLoadQueueTaskThread != null) {
            getBlockUpLoadQueueTaskThread.interrupt();
            if (Thread.currentThread() != getBlockUpLoadQueueTaskThread) {
                try {
                    getBlockUpLoadQueueTaskThread.join(5000);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted waiting for upload dispatcher", error);
                }
                if (getBlockUpLoadQueueTaskThread.isAlive()) {
                    throw new IllegalStateException("Upload dispatcher did not stop");
                }
            }
        }
    }


    //todo
    public void close() {
        closed = true;
        stopAllThread();
        blockUpdater.close();
        //关闭资源
        if (arena != null) {
            try {
                arena.close();
                arena = null;
                globalMemorySegment = null;
                log.info("Closed CacheBlockManager arena successfully.");
            } catch (Exception e) {
                log.error("Failed to close CacheBlockManager arena", e);
            }
        }
    }

    public int getBlockCount() {
        return blockCount;
    }


    public int getFreeBlockCount() {
        return freeBlocks.size();
    }

    public String getInstanceName() {
        return instanceName;
    }

}
