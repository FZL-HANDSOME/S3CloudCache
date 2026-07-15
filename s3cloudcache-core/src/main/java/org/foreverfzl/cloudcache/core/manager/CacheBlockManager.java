package org.foreverfzl.cloudcache.core.manager;

import org.foreverfzl.cloudcache.core.cache.AppendDataResult;
import org.foreverfzl.cloudcache.core.datastruct.BlockDataStruct;
import org.foreverfzl.cloudcache.core.cache.CloudCacheBlock;
import org.foreverfzl.cloudcache.metadata.entity.UploadTask;
import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.exception.CoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.*;

/**
 * 用于管理一个Bucket的所有Block，也可以理解为那个默认1GB的堆外缓冲区，也就是Block池
 */
public class CacheBlockManager {

    private static final Logger log = LoggerFactory.getLogger(LogName.CACHE_BLOCK_MANAGER);

    private final Arena arena;
    private final MemorySegment globalMemorySegment;
    private final int blockCount;
    public final String instanceName;
    public final String bucketName;
    //Bucket级别配置文件
    private final BucketConfig config;
    // 空闲/干净的 CloudCacheBlock 池
    private final BlockingQueue<CloudCacheBlock> freeBlocks;

    // 根据自定义 key 维护的 K-V 映射，key为fileFromOffset+BlockIndex
    private final ConcurrentHashMap<Long, CloudCacheBlock> keyBlockMap;
    //1024个锁分片，防止fileFromOffset+NameBlockIndex一个组合获取多个Block
    private final Object[] blockLocks = new Object[256];

    //block上传者
    private final CacheBlockUpdater blockUpdater;

    //该bucket对应的Block元数据管理者
    public BlockMetaDataManager blockMetaDataManager;

    //该线程专门获取BlockUpLoadQueueManager类中BlockUpLoadQueue中的任务
    private volatile boolean active = true;
    private final Thread getBlockUpLoadQueueTaskThread;

    public CacheBlockManager(String instanceName, String bucketName, BlockMetaDataManager blockMetaDataManager, S3Client s3Client, BucketConfig config) {
        this.config = config;
        this.blockCount = (int) (config.cacheSize / config.blockSize);
        this.instanceName = instanceName;
        this.bucketName = bucketName;
        // 1. 创建 MemorySegment 堆外缓冲区
        this.arena = Arena.ofShared();
        this.globalMemorySegment = arena.allocate(config.cacheSize);
        this.freeBlocks = new ArrayBlockingQueue<>(blockCount);
        this.keyBlockMap = new ConcurrentHashMap<>();
        this.blockUpdater = new CacheBlockUpdater(this, config.blockUpLoadCount, s3Client, config.enableHeadCheck);
        this.blockMetaDataManager = blockMetaDataManager;
        this.getBlockUpLoadQueueTaskThread = new Thread(this::getBlockUpLoadQueue);
        // 2. 初始化并维护所有的 CloudCacheBlock
        for (int i = 0; i < blockCount; i++) {
            long offset = (long) i * config.blockSize;
            CloudCacheBlock block = new CloudCacheBlock(offset, config.blockSize, globalMemorySegment.asSlice(offset, config.blockSize), this);
            freeBlocks.add(block);
        }
        for (int i = 0; i < blockLocks.length; i++) {
            blockLocks[i] = new Object();
        }
        getBlockUpLoadQueueTaskThread.start();
        log.info("Initialized CacheBlockManager with cacheSize={}, blockSize={}, blockCount={},blockUpLoadMaxCount={}",
                config.cacheSize, config.blockSize, blockCount, config.blockUpLoadCount);
    }


    private void getBlockUpLoadQueue() {
        while (active) {
            try {
                UploadTask task = blockMetaDataManager.getTaskFromUpLoadQueue();
                CloudCacheBlock cacheBlock = keyBlockMap.get(ProjectUtil.buildBlockKey(task.getFileFromOffset(), task.getLogicalIndex()));
                if (cacheBlock == null) {
                    return;
                }
                //上传
                if (cacheBlock.getCurReferenceCount() == 0) {
                    blockUpdater.upLoadBlock(cacheBlock);
                }
            } catch (InterruptedException e) {
                active=false;
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 往指定的Block中添加数据
     *
     * @param dataStruct 数据
     * @return 结果
     */
    public AppendDataResult appendData(BlockDataStruct dataStruct) {
        CloudCacheBlock cacheBlock = null;
        long curWritePosition = 0;
        int size = 0;
        long fileFromOffset = dataStruct.getFileFromOffset();
        int blockIndex = dataStruct.getBlockIndex();
        try {
            size = dataStruct.getDataLen();
            cacheBlock = getBlock(fileFromOffset, blockIndex);
            cacheBlock.getReference();
            //每个线程抢到自己的写指针
            curWritePosition = cacheBlock.tryAcquireWritePosition(size);
            MemorySegment cacheBlockSegment = cacheBlock.getWriteMemorySegment(curWritePosition, size);
            //将数据写入Block
            boolean isSuccess = dataStruct.writeTo(cacheBlockSegment);
            if (!isSuccess) {
                //失败后重试一次
                isSuccess = dataStruct.writeTo(cacheBlockSegment);
            }
            if (isSuccess) {
                blockMetaDataManager.addFinishedBytes(fileFromOffset, blockIndex, size);
            } else {
                log.warn("{} block write failed", cacheBlock.getS3Key());
                throw new CoreException("failed to write data in block");
            }
        } catch (Exception e) {
            //写入失败或者抛出异常，直接回收该block，拿一块新的block去WAL文件中恢复数据
            blockMetaDataManager.setMetaDataBroken(fileFromOffset, blockIndex);
            return AppendDataResult.fail(fileFromOffset, blockIndex);
        } finally {
            //写完后释放引用
            if (cacheBlock != null) cacheBlock.releaseReference();
        }
        return new AppendDataResult(cacheBlock.getS3Key(), curWritePosition, size, true,
                cacheBlock.getFileFromOffset(), cacheBlock.getLogicalIndex());
    }


    /**
     * 上传Block
     */
    public void updateBlock(CloudCacheBlock cacheBlock) {
        //将block元数据设置为上传中
        blockUpdater.upLoadBlock(cacheBlock);
    }

    /**
     * 获取一个可用并且干净的 CloudCacheBlock，并将其与指定的 cacheBlockKey 绑定。
     * 如果该 cacheBlockKey 已经关联了某个 Block，则直接返回已有的 Block。
     */
    public CloudCacheBlock getBlock(long fileFromOffset, int blockIndex) throws InterruptedException {
        long cacheBlockKey = ProjectUtil.buildBlockKey(fileFromOffset, blockIndex);
        // 检查是否已经存在与 cacheBlockKey 绑定的 block
        CloudCacheBlock existingBlock = keyBlockMap.get(cacheBlockKey);
        if (existingBlock != null) {
            return existingBlock;
        }
        synchronized (getLock(cacheBlockKey)) {
            // 双重检查
            existingBlock = keyBlockMap.get(cacheBlockKey);
            if (existingBlock != null) {
                return existingBlock;
            }
            // 否则获取一个干净的 block
            CloudCacheBlock block = freeBlocks.take();
            block.setS3Key(ProjectUtil.generateUniqueS3Key(config.s3KeyPrefix, this.instanceName, this.bucketName, fileFromOffset, blockIndex));
            block.setFileFromOffset(fileFromOffset);
            block.setLogicalIndex(blockIndex);
            keyBlockMap.put(cacheBlockKey, block);
            return block;
        }
    }

    /**
     * 将一个用完结束的旧 CloudCacheBlock 重新放入到 CloudCacheBlock 池中，并清理其属性和内存空间。
     */
    public void recycleBlock(CloudCacheBlock block) throws InterruptedException {
        if (block == null) {
            return;
        }
        // 2. 从 K-V 映射中移除
        long key = ProjectUtil.buildBlockKey(block.getFileFromOffset(), block.getLogicalIndex());
        keyBlockMap.remove(key);
        // 3. 清理 Block 的属性
        block.clean();
        // 4. 重新放入空闲池中
        freeBlocks.put(block);
    }

    private Object getLock(long key) {
        return blockLocks[(int) (key & 255)];
    }


    //todo
    public void close() {
        if (arena != null) {
            try {
                arena.close();
                log.info("Closed CacheBlockManager off-heap arena successfully.");
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
