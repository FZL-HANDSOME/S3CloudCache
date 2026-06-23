package org.foreverfzl.cloudcache.wal.global;

import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class WalInstanceBucketManager {
    private static final int LOCKS_COUNT = 128;
    private final ConcurrentHashMap<String, MappedFileManager> managerHashMap;
    private final String instanceName;
    private final S3CloudCacheConfig.WalConfig config;
    //用户指定的地址，如果用户没有指定则为默认
    private final String instanceDirPath;

    private final ReentrantLock[] locks;

    public WalInstanceBucketManager(String instanceName,String instanceDirPath, S3CloudCacheConfig.WalConfig config) {
        this.instanceName = instanceName;
        this.config = config;
        this.instanceDirPath=config.walPath!=null?
                config.walPath+ProjectUtil.WAL_FILE_ADDRESS:
                ProjectUtil.USER_HOME+ProjectUtil.WAL_FILE_ADDRESS;

        managerHashMap = new ConcurrentHashMap<>();
        locks = new ReentrantLock[LOCKS_COUNT];
        for (int i = 0; i < LOCKS_COUNT; i++) {
            locks[i] = new ReentrantLock();
        }
    }


    public void appendData(){

    }

    /**
     * 根据bucket获取对应的MappedFileManager
     */
    private MappedFileManager getBucketBlockManager(String bucketName) {
        // 第一次无锁查询
        MappedFileManager manager = managerHashMap.get(bucketName);
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
            // 创建新的MappedFileManager
            String managerDirPath=instanceDirPath+ File.separator+instanceName+File.separator+bucketName;
            manager = new MappedFileManager(instanceName,bucketName,managerDirPath,config.pageFlushLevel);
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
