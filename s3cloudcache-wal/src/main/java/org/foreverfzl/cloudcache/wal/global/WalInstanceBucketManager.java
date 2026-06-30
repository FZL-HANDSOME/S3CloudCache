package org.foreverfzl.cloudcache.wal.global;

import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.AppendMessageResult;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.foreverfzl.cloudchache.common.exception.WalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 管理所有的BucketManager
 */
public class WalInstanceBucketManager {

    protected static final Logger log = LoggerFactory.getLogger(LogName.WAL_INSTANCE_BUCKET_MANAGER);

    private static final int LOCKS_COUNT = 128;
    private final ConcurrentHashMap<String, MappedFileManager> managerHashMap;
    private final String instanceName;
    private final S3CloudCacheConfig config;
    //用户指定的地址，如果用户没有指定则为默认
    private final String instanceDirPath;

    private final ReentrantLock[] locks;

    public WalInstanceBucketManager(String instanceName, String instanceDirPath, S3CloudCacheConfig config) {
        this.instanceName = instanceName;
        this.config = config;
        this.instanceDirPath = instanceDirPath;
        managerHashMap = new ConcurrentHashMap<>();
        locks = new ReentrantLock[LOCKS_COUNT];
        for (int i = 0; i < LOCKS_COUNT; i++) {
            locks[i] = new ReentrantLock();
        }
    }


    //WalInstanceBucketManager的appendData只是路由作用
    public AppendMessageResult appendData(String bucketName, DataStruct dataStruct){
        MappedFileManager bucketFileManager = getOrCreateBucketFileManager(bucketName);
        if(bucketFileManager==null){
            throw new WalException("can not find MappedFileManager");
        }
        AppendMessageResult result = bucketFileManager.appendData(dataStruct);
        return result;
    }



    /**
     * 根据bucket获取对应的MappedFileManager，没有则创建
     */
    public MappedFileManager getOrCreateBucketFileManager(String bucketName) {
        MappedFileManager manager = managerHashMap.get(bucketName);
        if(manager!=null){
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
            // 创建新的MappedFileManager
            String managerDirPath = instanceDirPath + File.separator + instanceName + File.separator + bucketName;
            BucketConfig bucketConfig = config.specialBuckets.get(bucketName);
            BucketConfig realBucketConfig = bucketConfig != null ? bucketConfig : config.defaultBucketConfig;
            manager = new MappedFileManager(managerDirPath, instanceName, bucketName, realBucketConfig);
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
