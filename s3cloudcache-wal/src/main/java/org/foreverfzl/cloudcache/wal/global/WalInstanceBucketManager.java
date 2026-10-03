package org.foreverfzl.cloudcache.wal.global;

import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.foreverfzl.cloudchache.common.exception.WalException;
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
/**
 * 中文：实例级WAL管理器注册表，按Bucket串行创建并定时封口空闲尾块；拥有自己的调度器，不与其他实例共享关闭生命周期。
 * English: Instance-level registry of WAL managers, serializing bucket creation and periodically sealing idle tails. It owns its scheduler independently of other instances.
 */
public class WalInstanceBucketManager {

    /**
     * 中文：实例WAL维护日志。
     * English: Instance WAL maintenance logger.
     */
    protected static final Logger log = LoggerFactory.getLogger(LogName.WAL_INSTANCE_BUCKET_MANAGER);

    /**
     * 中文：128个分段锁，数量为2次幂以支持hash掩码。
     * English: 128 striped locks, a power of two for hash masking.
     */
    private static final int LOCKS_COUNT = 128;
    //key是bucketName
    /**
     * 中文：Bucket名称到该实例唯一文件管理器的映射。
     * English: Maps bucket names to their file managers within this instance.
     */
    private final ConcurrentHashMap<String, MappedFileManager> managerHashMap;
    /**
     * 中文：用于目录和日志的实例身份。
     * English: Instance identity used for directories and logs.
     */
    private final String instanceName;
    /**
     * 中文：创建Bucket管理器时读取的配置，快照/校验由上层实例负责。
     * English: Configuration used when creating bucket managers; the outer instance owns snapshotting and validation.
     */
    private final S3CloudCacheConfig config;
    //用户指定的地址，如果用户没有指定则为默认
    /**
     * 中文：实例目录，由walPath与instanceName组成。
     * English: Instance directory formed from walPath and instanceName.
     */
    private final String instanceDirPath;
    //主要防止多个线程同时创建FileManager
    /**
     * 中文：保护同Bucket首次创建，不串行化不同文件的业务写入。
     * English: Protects first creation per bucket without serializing business writes to different files.
     */
    private final ReentrantLock[] locks;
    //检查每个Bucket的最后一个Block的空闲时间的，如果超出配置的最大空闲时间则封口
    /**
     * 中文：空闲封口阈值，毫秒；实际检测还受调度间隔影响。
     * English: Idle-sealing threshold in milliseconds; detection also depends on scheduling cadence.
     */
    private final long blockMaxIdleTime;
    /**
     * 中文：实例自有单线程调度器；stopScheduler先停止它再允许最终资源关闭。
     * English: Instance-owned single-thread scheduler; stopScheduler terminates it before final resource teardown.
     */
    private final ScheduledExecutorService checkBlockMetaExecutor = Executors.newSingleThreadScheduledExecutor();


    /**
     * 中文：创建注册表和分段锁，并安排首次5秒、此后固定延迟15秒的空闲检查。
     * English: Creates the registry/striped locks and schedules idle checks after five seconds and then at a fixed fifteen-second delay.
     *
     * @param instanceName 中文：实例名称；English: instance name
     * @param config 中文：实例配置；English: instance configuration
     */
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
        //todo 记得改回15秒
        // 中文：当前实际调度参数为首次5秒、后续固定延迟15秒；上方TODO不改变运行行为。
        // English: Actual scheduling is five seconds initially and a fifteen-second fixed delay thereafter; the historical TODO does not alter runtime behavior.
        checkBlockMetaExecutor.scheduleWithFixedDelay(this::checkBlockMeta, 5, 15, TimeUnit.SECONDS);
    }

    /**
     * 中文：检查各Bucket当前活跃文件尾块，将封口结果位0/1/2分别转换为PageCache-ready、上传任务、恢复任务；异常只记录并留待后续周期。
     * English: Checks each active file's tail and translates seal-result bits 0/1/2 into page-cache readiness, upload tasks, and recovery tasks; errors are logged for later cycles.
     */
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
                if (activeFile == null) {
                    continue;
                }
                //获取最后活跃的文件各个属性
                int blockSize = fileManager.config.blockSize;
                long wrotePosition = activeFile.wrotePosition;
                long fileFromOffset = activeFile.fileFromOffset;
                int blockIndex = Math.toIntExact(ProjectUtil.divideByPower(wrotePosition, blockSize));
                //获取该fileManager对应的元数据管理者
                BlockMetaDataManager blockMetaDataManager = fileManager.blockMetaDataManager;
                BlockMetaData blockMetaData = blockMetaDataManager.getBlockMetaData(fileFromOffset, blockIndex);
                int state = blockMetaDataManager.chackLastActiveTime(blockMetaData, fileFromOffset, blockIndex, curTime, blockMaxIdleTime);
                if ((state & 1) == 1) {
                    //需要将对应文件中的block数组对应位置设置为1，方便刷盘
                    activeFile.setBlockStateArrayFinishedPageCache(blockIndex);
                }
                if ((state & (1 << 1)) == (1 << 1)) {
                    //需要将该任务提交到上传队列中
                    blockMetaDataManager.setTaskToUpdateQueue(fileFromOffset, blockIndex);
                }
                if ((state & (1 << 2)) == (1 << 2)) {
                    //需要提交到恢复队列中
                    blockMetaDataManager.setTaskToRecoverQueue(blockMetaData, fileFromOffset, blockIndex);
                }
            }
        } catch (Exception e) {
            log.warn("checkBlockMeta Task Failed", e);
        }
    }

    /**
     * 中文：只查找，不创建文件或线程。
     * English: Looks up without creating files or threads.
     *
     * @param bucketName 中文：Bucket名称；English: bucket name
     * @return 中文：已有管理器，未创建时为null；English: existing manager, or null if absent
     */
    public MappedFileManager onlyGetFileManager(String bucketName) {
        MappedFileManager fileManager = managerHashMap.get(bucketName);
        return fileManager;
    }

    //默认开始文件的起始位点为0
    /**
     * 中文：以0为请求起点创建/获取管理器；真正起点还会与持久化next-file-offset取最大值。
     * English: Gets/creates a manager with requested start zero; the actual start is maximized against persisted next-file-offset.
     *
     * @param bucketName 中文：Bucket名称；English: bucket name
     * @return 中文：该Bucket管理器；English: bucket manager
     */
    public MappedFileManager getOrCreateBucketFileManager(String bucketName) {
        return this.getOrCreateBucketFileManager(bucketName, 0L);
    }


    /**
     * 自定义创建manager，manager从fromOffset位点开始创建文件
     */
    /**
     * 中文：在Bucket分段锁下二次检查并创建；已有管理器直接返回，不重新应用fromOffset。
     * English: Double-checks and creates under a bucket stripe lock; an existing manager is returned without reapplying fromOffset.
     *
     * @param bucketName 中文：Bucket名称；English: bucket name
     * @param fromOffset 中文：请求的新WAL文件身份起点；English: requested new-WAL identity start
     * @return 中文：已有或新建管理器；English: existing or newly created manager
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
    /**
     * 中文：以Bucket哈希选择分段锁；哈希冲突只降低并发度。
     * English: Selects a lock by bucket hash; collisions only reduce concurrency.
     *
     * @param bucketName 中文：非null Bucket名称；English: non-null bucket name
     * @return 中文：该Bucket对应分段锁；English: stripe lock for the bucket
     */
    private ReentrantLock getLock(String bucketName) {
        return locks[bucketName.hashCode() & (LOCKS_COUNT - 1)];
    }

    /**
     * 中文：先停止空闲调度，再关闭各文件管理器；调用上层必须已处理在途写入、上传和恢复，不能把本方法当作业务排空。
     * English: Stops idle scheduling before closing file managers. The caller must already drain writes, uploads, and recovery; this method is not a business drain.
     */
    public void close() {
        stopScheduler();
        //关闭自己维护的bucketManager
        managerHashMap.forEach((bucketName, fileManager) -> {
            fileManager.close();
        });
    }

    /** 停止空闲封口任务，但保留所有文件映射供关闭刷盘和恢复使用。 */
    /**
     * 中文：发出中断并最多等待10秒；不关闭任何映射，供上层先停止封口再执行最终排空。未退出或被中断时抛异常。
     * English: Interrupts and waits up to ten seconds without closing mappings, allowing final draining after sealing stops. Throws if it does not stop or waiting is interrupted.
     *
     * @throws WalException 中文：调度器未及时停止或等待被中断；English: scheduler fails to stop in time or waiting is interrupted
     */
    public void stopScheduler() {
        checkBlockMetaExecutor.shutdownNow();
        try {
            if (!checkBlockMetaExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new WalException("Idle-block scheduler did not stop for " + instanceName);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new WalException("Interrupted while stopping idle-block scheduler for " + instanceName, e);
        }
    }
}
