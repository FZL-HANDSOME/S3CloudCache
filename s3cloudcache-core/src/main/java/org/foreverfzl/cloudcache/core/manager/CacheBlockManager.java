package org.foreverfzl.cloudcache.core.manager;

import org.foreverfzl.cloudcache.core.cache.AppendDataResult;
import org.foreverfzl.cloudcache.core.datastruct.BlockDataStruct;
import org.foreverfzl.cloudcache.core.cache.CloudCacheBlock;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.*;

/**
 * 用于管理一个Bucket的所有Block，也可以理解为那个默认1GB的堆外缓冲区，也就是Block池
 */
public class CacheBlockManager implements AutoCloseable {

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

    // 根据自定义 key 维护的 K-V 映射，key为fileName+BlockIndex
    private final ConcurrentHashMap<String, CloudCacheBlock> keyBlockMap;

    //block上传者
    private final CacheBlockUpdater blockUpdater;


    public CacheBlockManager(String instanceName, String bucketName, S3Client s3Client, BucketConfig config) {
        this.config = config;
        this.blockCount = (int) (config.cacheSize / config.blockSize);
        this.instanceName = instanceName;
        this.bucketName = bucketName;
        // 1. 创建 MemorySegment 堆外缓冲区
        this.arena = Arena.ofShared();
        this.globalMemorySegment = arena.allocate(config.cacheSize);
        this.freeBlocks = new ArrayBlockingQueue<>(blockCount);
        this.keyBlockMap = new ConcurrentHashMap<>();
        blockUpdater = new CacheBlockUpdater(this, config.blockUpLoadCount, s3Client);
        // 2. 初始化并维护所有的 CloudCacheBlock
        for (int i = 0; i < blockCount; i++) {
            long offset = (long) i * config.blockSize;
            CloudCacheBlock block = new CloudCacheBlock(offset, config.blockSize, globalMemorySegment.asSlice(offset, config.blockSize), this);
            freeBlocks.add(block);
        }
        log.info("Initialized CacheBlockManager with cacheSize={}, blockSize={}, blockCount={},blockUpLoadMaxCount={}",
                config.cacheSize, config.blockSize, blockCount, config.blockUpLoadCount);
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
        try {
            size = dataStruct.getDataLen();
            cacheBlock = getBlock(dataStruct.getFileName(), dataStruct.getBlockIndex());
            cacheBlock.getReference();
            //每个线程抢到自己的写指针
            curWritePosition = cacheBlock.tryAcquireWritePosition(size);
            MemorySegment cacheBlockSegment = cacheBlock.getMemorySegment(curWritePosition,size);
            //将数据写入Block
            dataStruct.writeTo(cacheBlockSegment);
        } catch (Exception e) {
            return AppendDataResult.fail();
        } finally {
            //写完后释放引用
            if (cacheBlock != null) cacheBlock.releaseReference();
        }
        return new AppendDataResult(cacheBlock.getS3Key(), curWritePosition, size, true);
    }

    /**
     * 上传Block
     */
    public void updateBlock(CloudCacheBlock cacheBlock) {
        blockUpdater.upLoadBlock(cacheBlock);
    }


    /**
     * 获取一个可用并且干净的 CloudCacheBlock，并将其与指定的 cacheBlockKey 绑定。
     * 如果该 cacheBlockKey 已经关联了某个 Block，则直接返回已有的 Block。
     */
    private CloudCacheBlock getBlock(String fileName, int blockIndex) throws InterruptedException {
        String cacheBlockKey = fileName + "_" + blockIndex;
        // 检查是否已经存在与 cacheBlockKey 绑定的 block
        CloudCacheBlock existingBlock = keyBlockMap.get(cacheBlockKey);
        if (existingBlock != null) {
            return existingBlock;
        }
        synchronized (cacheBlockKey.intern()) {
            // 双重检查
            existingBlock = keyBlockMap.get(cacheBlockKey);
            if (existingBlock != null) {
                return existingBlock;
            }
            // 否则获取一个干净的 block
            CloudCacheBlock block = freeBlocks.take();
            block.setS3Key(ProjectUtil.generateUniqueS3Key(config.s3KeyPrefix, this.instanceName, this.bucketName, fileName, blockIndex));
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
        String key = block.getMappedFile().getFileName() + "_" + block.getLogicalIndex();
        keyBlockMap.remove(key);
        // 3. 清理 Block 的属性
        block.clean();
        // 4. 重新放入空闲池中
        freeBlocks.put(block);
    }


    //todo
    @Override
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
