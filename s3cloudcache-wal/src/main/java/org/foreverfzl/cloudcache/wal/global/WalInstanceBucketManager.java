package org.foreverfzl.cloudcache.wal.global;

import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 管理所有的BucketManager
 */
public class WalInstanceBucketManager {

    protected static final Logger log = LoggerFactory.getLogger(LogName.WAL_INSTANCE_BUCKET_MANAGER);

    private static final int LOCKS_COUNT = 128;
    //key是bucketName
    private final ConcurrentHashMap<String, MappedFileManager> managerHashMap;
    private final String instanceName;
    private final S3CloudCacheConfig config;
    //用户指定的地址，如果用户没有指定则为默认
    private final String instanceDirPath;
    //主要防止多个线程同时创建FileManager
    private final ReentrantLock[] locks;
    //检查每个Bucket的最后一个Block的空闲时间的，如果超出配置的最大空闲时间则封口
    private final long blockMaxIdleTime;
    private static final ScheduledExecutorService checkBlockMetaExecutor = Executors.newSingleThreadScheduledExecutor();


    public WalInstanceBucketManager(String instanceName, S3CloudCacheConfig config) {
        this.instanceName = instanceName;
        this.config = config;
        this.instanceDirPath = config.walPath + File.separator + instanceName;
        this.blockMaxIdleTime = config.blockMaxIdleTime;
        managerHashMap = new ConcurrentHashMap<>();
        locks = new ReentrantLock[LOCKS_COUNT];
        for (int i = 0; i < LOCKS_COUNT; i++) {
            locks[i] = new ReentrantLock();
        }
//        checkBlockMetaExecutor.scheduleAtFixedRate(this::checkBlockMeta, 5, 15, TimeUnit.SECONDS);
    }

    private void checkBlockMeta() {
        try {
            //获取该instance下的所有bucketManager
            Collection<MappedFileManager> values = managerHashMap.values();
            if (values.isEmpty()) {
                return;
            }
            //开始检查
            long curTime = System.currentTimeMillis();
            for (MappedFileManager fileManager : values) {
                //获取到当前Bucket的活跃文件
                DefaultMappedFile activeFile = fileManager.getActiveMappedFile().get();
                //获取最后活跃的文件各个属性
                int blockSize = fileManager.config.blockSize;
                long wrotePosition = activeFile.wrotePosition;
                int blockIndex = Math.toIntExact(ProjectUtil.divideByPower(wrotePosition, blockSize));
                //获取该fileManager对应的元数据管理者
                BlockMetaDataManager blockMetaDataManager = fileManager.blockMetaDataManager;
                blockMetaDataManager.chackLastActiveTime(activeFile.fileFromOffset, blockIndex, curTime, blockMaxIdleTime);
            }
        } catch (Exception e) {
            log.warn("checkBlockMeta Task Failed", e);
        }
    }

    public MappedFileManager onlyGetFileManager(String bucketName) {
        MappedFileManager fileManager = managerHashMap.get(bucketName);
        return fileManager;
    }

    //默认开始文件的起始位点为0
    public MappedFileManager getOrCreateBucketFileManager(String bucketName) {
        return this.getOrCreateBucketFileManager(bucketName, 0L);
    }


    /**
     * 自定义创建manager，manager从fromOffset位点开始创建文件
     */
    public MappedFileManager getOrCreateBucketFileManager(String bucketName, long fromOffset) {
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
            String managerDirPath = instanceDirPath + File.separator + bucketName;
            manager = new MappedFileManager(managerDirPath, instanceName, bucketName, config.getBucketConfig(bucketName), fromOffset);
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

    public void close() {
        //关闭定时线程池
        checkBlockMetaExecutor.shutdownNow();
        //关闭自己维护的bucketManager
        managerHashMap.forEach((bucketName, fileManager) -> {
            fileManager.close();
        });
    }
}
