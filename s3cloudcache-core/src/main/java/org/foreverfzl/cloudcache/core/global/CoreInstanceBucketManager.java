package org.foreverfzl.cloudcache.core.global;

import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 因为一个Instance可能发送多个Bucket，该类就是管理该Instance下所有的Bucket
 */
/**
 * 中文：按 bucket 名管理一个实例内的物理块管理器；只串行化同分段的首次创建，关闭前仍由上层停止业务准入。
 * English: Manages per-bucket physical block managers for one instance; it serializes initial creation by stripe, while callers must stop business admission before closing.
 */
public class CoreInstanceBucketManager {

    /**
     * 中文：bucket 名到已创建管理器的注册表；删除条目不隐含关闭资源。
     * English: Registry from bucket name to created manager; removing an entry does not close its resources.
     */
    private final ConcurrentHashMap<String, CacheBlockManager> managerHashMap;

    /**
     * 中文：首次创建使用的分段锁数量，必须为二的幂以配合位掩码索引。
     * English: Number of creation-lock stripes; a power of two is required by the mask-based index.
     */
    private static final int LOCKS_COUNT = 128;
    /**
     * 中文：首次创建锁数组；不同 bucket 可能共享一个分段。
     * English: Initial-creation lock array; different buckets may share a stripe.
     */
    private final ReentrantLock[] locks; //保证同一时刻只有一个线程创建Manager
    /**
     * 中文：所属实例名称，传递给各 bucket 的资源与日志上下文。
     * English: Owning instance name, propagated to bucket resource and logging context.
     */
    private final String instanceName;
    /**
     * 中文：借用的同步 S3 客户端，本管理器不负责关闭。
     * English: Borrowed synchronous S3 client, not closed by this manager.
     */
    private final S3Client s3Client;
    /**
     * 中文：调用方提供的配置引用；本类不再次复制，已创建 manager 不随新调用参数重新配置。
     * English: Configuration reference supplied by the caller; this class does not copy it or reconfigure existing managers on later calls.
     */
    private final S3CloudCacheConfig config;


    /**
     * 中文：初始化注册表与创建锁，尚不分配 bucket 堆外块。
     * English: Initializes the registry and creation locks without allocating bucket-native blocks.
     * @param instanceName 实例名称；English: instance name
     * @param s3Client 借用的 S3 客户端；English: borrowed S3 client
     * @param config 实例配置；English: instance configuration
     */
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


    /**
     * 中文：只查找已经创建的管理器，不分配资源。
     * English: Looks up an existing manager without allocating resources.
     * @param bucketName bucket 名称；English: bucket name
     * @return 管理器或 null；English: manager or null
     */
    public CacheBlockManager onlyGetBlockManager(String bucketName) {
        return managerHashMap.get(bucketName);
    }

    /**
     * 中文：使用 bucket 配置获取或创建管理器，并在首次创建时启用上传队列线程。
     * English: Gets or creates a manager from bucket configuration, enabling the upload-queue worker on initial creation.
     * @param bucketName bucket 名称；English: bucket name
     * @param blockMetaDataManager 与 WAL 共享的元数据管理器；English: metadata manager shared with WAL
     * @return 已存在或新建的管理器；English: existing or newly created manager
     */
    public CacheBlockManager getOrCreateBlockManager(String bucketName, BlockMetaDataManager blockMetaDataManager) {
        BucketConfig bucketConfig = config.getBucketConfig(bucketName);
        return this.getOrCreateBlockManager(bucketName, blockMetaDataManager, bucketConfig, bucketConfig.cacheSize,
                bucketConfig.blockSize, bucketConfig.blockUpLoadCount, true);
    }


    /**
     * 根据bucket获取对应的BlockManager
     */
    /**
     * 中文：双重检查后在创建锁下初始化管理器；已有条目直接返回，不应用本次不同的配置参数。
     * English: Initializes a manager under its creation lock after double checking; an existing entry is returned without applying different arguments.
     * @param bucketName bucket 名称；English: bucket name
     * @param blockMetaDataManager 共享元数据管理器；English: shared metadata manager
     * @param bucketConfig bucket 配置，null 时使用实例配置；English: bucket configuration, or instance fallback when null
     * @param cacheSize 总缓存字节数；English: total cache bytes
     * @param cacheBlockSize 单物理块字节数；English: bytes per physical block
     * @param blockUpLoadCount 最大并发上传数；English: maximum concurrent uploads
     * @param isCreateThread 是否创建上传队列消费线程；English: whether to create an upload-queue worker
     * @return bucket 对应的唯一已注册管理器；English: registered manager for the bucket
     */
    public CacheBlockManager getOrCreateBlockManager(String bucketName, BlockMetaDataManager blockMetaDataManager, BucketConfig bucketConfig,
                                                     long cacheSize, int cacheBlockSize, int blockUpLoadCount, boolean isCreateThread) {
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
            if (bucketConfig == null) bucketConfig = config.getBucketConfig(bucketName);
            manager = new CacheBlockManager(instanceName, bucketName, blockMetaDataManager, s3Client, bucketConfig,
                    cacheSize, cacheBlockSize, blockUpLoadCount, isCreateThread);
            managerHashMap.put(bucketName, manager);
            return manager;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 中文：仅移除注册表条目；调用方仍负责关闭该管理器并协调所有使用者。
     * English: Removes only the registry entry; callers remain responsible for closing the manager and coordinating its users.
     * @param bucketName 要移除的 bucket 名称；English: bucket name to remove
     */
    public void removeBlockManager(String bucketName) {
        managerHashMap.remove(bucketName);
    }

    //通过hash分桶获取对应的Lock对象
    /**
     * 中文：通过 bucket 哈希选择创建锁，不承诺不同 bucket 一定使用不同锁。
     * English: Selects a creation lock by bucket hash; different buckets may use the same lock.
     * @param bucketName bucket 名称；English: bucket name
     * @return 对应分段锁；English: corresponding striped lock
     */
    private ReentrantLock getLock(String bucketName) {
        return locks[bucketName.hashCode() & (LOCKS_COUNT - 1)];
    }

    /**
     * 中文：依次关闭当前注册的各 bucket 管理器，遇到抛出的异常会中断本次遍历；不清空注册表或关闭共享 S3 客户端，上层需先完成写入与恢复的停机协调。
     * English: Closes currently registered bucket managers, aborting traversal if one throws; does not clear the registry or close the shared S3 client, and callers must first coordinate writer and recovery shutdown.
     */
    public void close() {
        managerHashMap.forEach((cacheName, manager) -> {
            manager.close();
        });
    }
}
