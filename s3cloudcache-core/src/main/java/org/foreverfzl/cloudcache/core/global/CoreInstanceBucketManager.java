package org.foreverfzl.cloudcache.core.global;

import org.foreverfzl.cloudcache.core.cache.AppendDataResult;
import org.foreverfzl.cloudcache.core.datastruct.BlockDataStruct;
import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.foreverfzl.cloudchache.common.exception.WalException;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 因为一个Instance可能发送多个Bucket，该类就是管理该Instance下所有的Bucket
 */
public class CoreInstanceBucketManager {

    private final ConcurrentHashMap<String, CacheBlockManager> managerHashMap;

    private static final int LOCKS_COUNT = 128;
    private final ReentrantLock[] locks; //保证同一时刻只有一个线程创建Manager
    private final String instanceName;
    private final S3Client s3Client;
    private final S3CloudCacheConfig config;



    public CoreInstanceBucketManager(String instanceName, S3Client s3Client, S3CloudCacheConfig config) {
        this.instanceName = instanceName;
        this.s3Client = s3Client;
        this.config = config;
        managerHashMap = new ConcurrentHashMap<>();
        locks = new ReentrantLock[LOCKS_COUNT];
        for (int i = 0; i < LOCKS_COUNT; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    public AppendDataResult appendData(String bucketName, BlockDataStruct dataStruct) {
        CacheBlockManager blockManager = getOrCreateBlockManager(bucketName);
        if (blockManager == null) {
            throw new WalException("can not find blockManager");
        }
        AppendDataResult appendDataResult = blockManager.appendData(dataStruct);
        if (!appendDataResult.result()) {
            //失败后重试
            appendDataResult=blockManager.failToReAppendData(dataStruct,appendDataResult.offset(),appendDataResult.size());
        }
        //如果失败了
        return appendDataResult;
    }

    /**
     * 根据bucket获取对应的BlockManager，没有则创建
     */
    public CacheBlockManager getOrCreateBlockManager(String bucketName) {
        // 第一次无锁查询
        CacheBlockManager manager = managerHashMap.get(bucketName);
        if (manager != null) {
            return manager;
        }
        // 获取该bucket对应的分段锁
        ReentrantLock lock = getLock(bucketName);
        lock.lock();
        try {
            // 双重检查
            manager = managerHashMap.get(bucketName);
            if (manager != null) {
                return manager;
            }
            // 创建新的BlockManager
            BucketConfig bucketConfig = config.specialBuckets.get(bucketName);
            BucketConfig realConfig = bucketConfig != null ? bucketConfig : config.defaultBucketConfig;
            manager = new CacheBlockManager(instanceName, bucketName, s3Client, realConfig);
            managerHashMap.put(bucketName, manager);
            return manager;
        } finally {
            lock.unlock();
        }
    }

    //通过hash分桶获取对应的Lock对象
    private ReentrantLock getLock(String bucketName) {
        return locks[bucketName.hashCode() & (LOCKS_COUNT - 1)];
    }
}
