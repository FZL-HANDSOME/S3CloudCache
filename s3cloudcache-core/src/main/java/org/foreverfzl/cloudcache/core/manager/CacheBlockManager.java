package org.foreverfzl.cloudcache.core.manager;

import org.foreverfzl.cloudcache.core.cache.AppendDataResult;
import org.foreverfzl.cloudcache.core.cache.CacheBlockReferenceResource;
import org.foreverfzl.cloudcache.core.cache.CloudCacheBlock;
import org.foreverfzl.cloudcache.core.datastruct.BlockDataStruct;
import org.foreverfzl.cloudcache.metadata.entity.UploadTask;
import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
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
    //1024个锁分片，防止fileFromOffset+NameBlockIndex一个组合获取多个Block
    private final Object[] blockLocks = new Object[256];

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
        for (int i = 0; i < blockLocks.length; i++) {
            blockLocks[i] = new Object();
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
                    return;
                }
                //上传
                blockUpdater.upLoadBlock(cacheBlock);
            } catch (InterruptedException e) {
                active = false;
                break;
            }
        }
    }

    public AppendDataResult appendData(BlockDataStruct dataStruct) {
        return this.appendData(dataStruct, config.s3KeyPrefix);
    }

    public AppendDataResult appendDataText(BlockDataStruct dataStruct) {
        return this.appendDataText(dataStruct, config.s3KeyPrefix);
    }

    public AppendDataResult appendDataText(BlockDataStruct dataStruct, String prefix) {
        CloudCacheBlock cacheBlock = null;
        long curWritePosition = 0;
        int size = 0;
        long fileFromOffset = dataStruct.getFileFromOffset();
        int blockIndex = dataStruct.getBlockIndex();
        try {
            DefaultMappedFile defaultMappedFile = dataStruct.getDefaultMappedFile();
            size = dataStruct.getDataLen();
            cacheBlock = getBlock(fileFromOffset, blockIndex, prefix, defaultMappedFile);
        } catch (InterruptedException e) {
            log.warn("can not get block , fileOffset={}, blockIndex={}", fileFromOffset, blockIndex);
            return AppendDataResult.fail(null,fileFromOffset, blockIndex);
        }
        if (!cacheBlock.isActive()) {
            return AppendDataResult.fail(cacheBlock.getS3Key(),fileFromOffset, blockIndex);
        }
        cacheBlock.getReference();
        boolean isError = false;
        writeCount.incrementAndGet();
        try {
            //每个线程抢到自己的写指针
            curWritePosition = cacheBlock.tryAcquireWritePosition(size);
            MemorySegment cacheBlockSegment = cacheBlock.getWriteMemorySegment(curWritePosition, size);
            if (!cacheBlock.isActive()) {
                return AppendDataResult.fail(cacheBlock.getS3Key(),fileFromOffset, blockIndex);
            }
            boolean isSuccess = dataStruct.writeTo(cacheBlockSegment);
            if (!isSuccess) {
                //失败后重试一次
                isSuccess = dataStruct.writeTo(cacheBlockSegment);
            }
            if (isSuccess) {
                blockMetaDataManager.addFinishedBytes(fileFromOffset, blockIndex, size);
            } else {
                throw new CoreException("failed to write data in block");
            }
        } catch (Exception e) {
            log.error("{} block write failed", cacheBlock.getS3Key(), e);
            isError = true;
            //写入失败或者抛出异常，我们在finally中将block标记为broken并且标记为clean
            return AppendDataResult.fail(cacheBlock.getS3Key(),fileFromOffset, blockIndex);
        } finally {
            if (isError) {
                cacheBlock.setUnActive();
                cacheBlock.setBroken();
            }
            //写完后释放引用
            cacheBlock.releaseReference();
            writeCount.decrementAndGet();
        }
        return new AppendDataResult(cacheBlock.getS3Key(), curWritePosition, size, true,
                cacheBlock.getFileFromOffset(), cacheBlock.getLogicalIndex());
    }


    /**
     * 往指定的Block中添加数据
     *
     * @param dataStruct 数据
     * @return 结果
     */
    int count = 0;

    public AppendDataResult appendData(BlockDataStruct dataStruct, String prefix) {
        CloudCacheBlock cacheBlock = null;
        long curWritePosition = 0;
        int size = 0;
        long fileFromOffset = dataStruct.getFileFromOffset();
        int blockIndex = dataStruct.getBlockIndex();
        try {
            DefaultMappedFile defaultMappedFile = dataStruct.getDefaultMappedFile();
            size = dataStruct.getDataLen();
            cacheBlock = getBlock(fileFromOffset, blockIndex, prefix, defaultMappedFile);
        } catch (InterruptedException e) {
            log.warn("can not get block , fileOffset={}, blockIndex={}", fileFromOffset, blockIndex);
            return AppendDataResult.fail(null,fileFromOffset, blockIndex);
        }
        if (!cacheBlock.isActive()) {
            return AppendDataResult.fail(cacheBlock.getS3Key(),fileFromOffset, blockIndex);
        }
        cacheBlock.getReference();
        boolean isError = false;
        writeCount.incrementAndGet();
        try {
            //每个线程抢到自己的写指针
            curWritePosition = cacheBlock.tryAcquireWritePosition(size);
            MemorySegment cacheBlockSegment = cacheBlock.getWriteMemorySegment(curWritePosition, size);
            //将数据写入Block
            if (count == 1) {
                throw new CoreException("failed to write data in block");
            }
            if (!cacheBlock.isActive()) {
                return AppendDataResult.fail(cacheBlock.getS3Key(),fileFromOffset, blockIndex);
            }
            boolean isSuccess = dataStruct.writeTo(cacheBlockSegment);
            if (!isSuccess) {
                //失败后重试一次
                isSuccess = dataStruct.writeTo(cacheBlockSegment);
            }
            if (isSuccess) {
                blockMetaDataManager.addFinishedBytes(fileFromOffset, blockIndex, size);
            } else {
                throw new CoreException("failed to write data in block");
            }
        } catch (Exception e) {
            log.error("{} block write failed", cacheBlock.getS3Key(), e);
            isError = true;
            //写入失败或者抛出异常，我们在finally中将block标记为broken并且标记为clean
            return AppendDataResult.fail(cacheBlock.getS3Key(),fileFromOffset, blockIndex);
        } finally {
            if (isError) {
                cacheBlock.setUnActive();
                cacheBlock.setBroken();
            }
            //写完后释放引用
            cacheBlock.releaseReference();
            writeCount.decrementAndGet();
            count++;
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
            CloudCacheBlock block = entry.getValue();
            if (block.isBroken()) {
                continue;
            }
            updateBlock(block);
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
            block.setBlockMetaData(blockMetaDataManager.getOrCreate(fileFromOffset, blockIndex));
            block.setS3Key(ProjectUtil.generateUniqueS3Key(prefix, this.instanceName, this.bucketName, fileFromOffset, blockIndex));
            block.setFileFromOffset(fileFromOffset);
            block.setLogicalIndex(blockIndex);
            block.setDefaultMappedFile(defaultMappedFile);
            keyBlockMap.put(cacheBlockKey, block);
            return block;
        }
    }


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


    public CloudCacheBlock getExistingBlock(long fileFromOffset, int blockIndex) {
        return keyBlockMap.get(ProjectUtil.buildBlockKey(fileFromOffset, blockIndex));
    }

    /**
     * 清除block并且放回到空闲池中
     *
     * @param block
     */
    public void cleanAndRecycle(CloudCacheBlock block) {
        if (block == null) {
            return;
        }
        try {
            // 从 K-V 映射中移除
            long key = ProjectUtil.buildBlockKey(block.getFileFromOffset(), block.getLogicalIndex());
            keyBlockMap.remove(key);
            block.clean();
            // 重新放入空闲池中
            freeBlocks.put(block);
        } catch (InterruptedException e) {
            log.warn("{}block was interrupted during put in cleanAndRecycle method", block);
        }
    }

    public void cleanAndRecycleWithLock(CloudCacheBlock block) {
        if (block == null) {
            return;
        }
        if (!block.isDelayClean()) {
            return;
        }
        try {
            // 从 K-V 映射中移除
            long key = ProjectUtil.buildBlockKey(block.getFileFromOffset(), block.getLogicalIndex());
            synchronized (getLock(key)) {
                if (!block.isDelayClean()) {
                    return;
                }
                keyBlockMap.remove(key);
                block.clean();
                // 重新放入空闲池中
                freeBlocks.put(block);
            }
        } catch (InterruptedException e) {
            log.warn("{}block was interrupted during put in cleanAndRecycleWithLock method", block);
        }
    }

    private Object getLock(long key) {
        return blockLocks[(int) (key & 255)];
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
        getBlockUpLoadQueueTaskThread.interrupt();
    }


    //todo
    public void close() {
        getBlockUpLoadQueueTaskThread.interrupt();
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
