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
/**
 * 中文：一个 bucket 的固定容量堆外块池与生命周期协调器，实际容量由参数指定；逻辑块身份、准入、回收在同一 BlockMetaData 监视锁下协调。
 * English: Fixed-capacity native block pool and lifecycle coordinator for one bucket; capacity is configured, while logical identity, admission, and recycling are coordinated under the same BlockMetaData monitor.
 */
public class CacheBlockManager {

    /**
     * 中文：块池分配、写入失败及停机日志。
     * English: Logs pool allocation, append failures, and shutdown.
     */
    private static final Logger log = LoggerFactory.getLogger(LogName.CACHE_BLOCK_MANAGER);

    //管理的的堆外内存
    /**
     * 中文：管理器拥有的共享 Arena；仅在上传执行器终止且上层已停稳写入、恢复后关闭。
     * English: Manager-owned shared arena; close only after upload termination and upstream writer/recovery quiescence.
     */
    private Arena arena;
    /**
     * 中文：Arena 内的总内存区域，各物理块仅借用其不重叠切片。
     * English: Root allocation in the arena; physical blocks borrow disjoint slices.
     */
    private MemorySegment globalMemorySegment;

    /**
     * 中文：池容量，等于总缓存字节数除以单块字节数向下取整。
     * English: Pool capacity, computed by integer division of cache bytes by block bytes.
     */
    private final int blockCount;
    /**
     * 中文：所属实例名称，用于对象键和诊断。
     * English: Owning instance name used for object keys and diagnostics.
     */
    public final String instanceName;
    /**
     * 中文：此池唯一对应的 bucket 名称。
     * English: Name of the single bucket served by this pool.
     */
    public final String bucketName;
    //Bucket级别配置文件
    /**
     * 中文：调用方传入的 bucket 配置引用，本类不自行复制。
     * English: Caller-supplied bucket configuration reference; this class does not copy it.
     */
    private final BucketConfig config;
    // 空闲/干净的 CloudCacheBlock 池
    /**
     * 中文：尚未绑定逻辑任务的物理块；容量不足形成背压，清理不擦除旧内存字节。
     * English: Physical blocks not bound to logical tasks; exhaustion provides backpressure, and cleanup does not erase old bytes.
     */
    private final BlockingQueue<CloudCacheBlock> freeBlocks;

    // 根据自定义 key 维护的 K-V 映射，key为fileFromOffset+BlockIndex
    /**
     * 中文：编码后的 WAL 文件偏移与块序号到物理块的绑定；取得引用前须在 metadata 锁下复核映射。
     * English: Bindings from encoded WAL offset/index to physical blocks; revalidate under metadata before acquiring a reference.
     */
    private final ConcurrentHashMap<Long, CloudCacheBlock> keyBlockMap;
    //block上传者
    /**
     * 中文：本管理器拥有的上传调度器，关闭 Arena 前必须终止其任务。
     * English: Owned upload coordinator whose tasks must terminate before arena closure.
     */
    private final CacheBlockUpdater blockUpdater;

    //该bucket对应的Block元数据管理者
    /**
     * 中文：与该 bucket 的 WAL 层共享的状态、队列及 Future 注册表管理器。
     * English: Manager of state, queues, and Future registrations shared with this bucket's WAL layer.
     */
    public BlockMetaDataManager blockMetaDataManager;

    //正在上传的数量
    /**
     * 中文：已登记但未释放资源的上传任务数，包含等待信号量的任务，不包含用户通知回调。
     * English: Registered uploads whose resources have not been released, including permit waiters but excluding user notification callbacks.
     */
    protected AtomicInteger upCount = new AtomicInteger(0);

    //正在写入的数量
    /**
     * 中文：进入本类 append 的请求数，包含等待物理块者，不统计此前的 WAL 写入阶段。
     * English: Active appends in this class, including pool waiters but excluding the preceding WAL phase.
     */
    protected AtomicInteger writeCount = new AtomicInteger(0);

    //该线程专门获取BlockUpLoadQueueManager类中BlockUpLoadQueue中的任务
    /**
     * 中文：上传队列消费线程的运行标志，不是业务写入的完整准入闸门。
     * English: Run flag for the upload-queue worker, not a complete business-write admission gate.
     */
    private volatile boolean active = true;
    /**
     * 中文：阻止继续分配物理块的关闭标志；已取得租约的工作需由上层先停稳。
     * English: Closing flag preventing further allocation; upstream must first quiesce work already holding leases.
     */
    private volatile boolean closed;
    /**
     * 中文：可选的上传队列消费者，未启用时为 null，由本管理器停止并等待。
     * English: Optional upload-queue consumer, null when disabled; stopped and joined by this manager.
     */
    private Thread getBlockUpLoadQueueTaskThread;


    /**
     * 中文：分配共享堆外区域、切分固定物理池并按需启动上传队列线程；参数合法性由上层配置校验配合保障。
     * English: Allocates shared native memory, partitions a fixed physical pool, and optionally starts the upload-queue worker; callers also validate configuration.
     * @param instanceName 实例名称；English: instance name
     * @param bucketName bucket 名称；English: bucket name
     * @param blockMetaDataManager 与 WAL 共享的元数据管理器；English: metadata manager shared with WAL
     * @param s3Client 借用的同步 S3 客户端；English: borrowed synchronous S3 client
     * @param config bucket 配置引用；English: bucket configuration reference
     * @param cacheSize 总内存字节数；English: total allocation size in bytes
     * @param cacheBlockSize 单块容量字节数；English: capacity per physical block in bytes
     * @param blockUpLoadCount 最大并发上传数；English: maximum concurrent uploads
     * @param isCreateThread 是否启动上传队列消费线程；English: whether to start the upload-queue worker
     */
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


    /**
     * 中文：阻塞消费上传候选任务，忽略已无绑定的陈旧任务；真正上传资格仍由 updater 原子复核，中断结束循环。
     * English: Consumes upload candidates, skipping stale tasks without a binding; the updater atomically rechecks eligibility, and interruption ends the loop.
     */
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

    /**
     * 中文：使用 bucket 配置中的键前缀执行物理块追加，返回值不代表 S3 已提交。
     * English: Appends to a physical block using the configured key prefix; its result does not mean S3 commit.
     * @param dataStruct 已具有 WAL 定位的数据视图；English: data view with WAL coordinates
     * @param futureContext 正常请求的 Future 上下文，重放时可为 null；English: request Future context, nullable during replay
     * @param isAddFuture 是否登记原始请求及其完成到达字节数；English: whether to register an original request and account its settled bytes
     * @return 本次物理复制结果；English: physical-copy result for this invocation
     */
    public AppendDataResult appendData(BlockDataStruct dataStruct, FutureContext futureContext, boolean isAddFuture) {
        return this.appendData(dataStruct, config.s3KeyPrefix, futureContext, isAddFuture);
    }

    /**
     * 往指定的Block中添加数据
     *
     * @param dataStruct 数据
     * @return 结果
     */

    /**
     * 中文：将已定位的 WAL value 复制到物理块，writeTo 返回 false 时重试一次，抛出 Exception 则直接失败结算；正常请求先登记 Future，最后无论成功与否都结算到达字节数。损坏块隔离等待完整重放，用户成功仅由上传提交后通知。
     * English: Copies a WAL-located value into a physical block, retrying once only when writeTo returns false; an Exception enters failure settlement directly. Original requests register their Future first and account settled bytes in all outcomes. Broken blocks remain quarantined for full replay; user success is notified only after upload commit.
     * @param dataStruct 数据视图，必须提供有效 WAL 文件与逻辑块坐标；English: data view with a valid WAL file and logical block coordinates
     * @param prefix 生成当前逻辑块对象键的前缀；English: prefix used for the logical block object key
     * @param futureContext Future 上下文，isAddFuture 为 true 时不得为 null；English: Future context, required when isAddFuture is true
     * @param isAddFuture 正常请求为 true，重放为 false，避免重复登记与累计；English: true for original requests and false for replay to avoid duplicate registration/accounting
     * @return 物理追加成功或失败；普通复制或分配异常转为失败结果，中断标志会恢复；English: physical append success or failure; ordinary copy/allocation exceptions become failure results and interruption is restored
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
                    // 中文：查询得到的物理对象可能已复用，只有原 metadata 和映射均一致才能取得写入租约。
                    // English: A looked-up physical object may already be reused; acquire a writer lease only when both metadata identity and mapping still match.
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
            // 中文：引用保护绑定与内存生命周期，实际复制不持 metadata 锁，以允许不同预留区并行写入。
            // English: The reference protects binding and memory lifetime; copying runs outside metadata so disjoint reservations can be written concurrently.
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
                        // 中文：没有有效物理绑定时不能安排原地恢复；终结逻辑任务，但不确认或删除其 WAL。
                        // English: Without a valid physical binding, in-place recovery is impossible; terminate the logical task without acknowledging or deleting its WAL.
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
                    // 中文：正常请求即使被损坏块拒绝也计入已到达字节，防止恢复遗漏 WAL 到 core 之间的迟到请求。
                    // English: Count original requests even when rejected by a broken block, so recovery cannot overtake late WAL-to-core arrivals.
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
    /**
     * 中文：尝试提交一个上传候选；不满足条件时无操作，不在此方法直接完成 Future。
     * English: Attempts to submit an upload candidate; ineligible blocks are ignored and this method does not directly complete Futures.
     * @param cacheBlock 当前候选物理块，可为 null；English: candidate physical block, nullable
     */
    public void updateBlock(CloudCacheBlock cacheBlock) {
        blockUpdater.upLoadBlock(cacheBlock);
    }

    /**
     * 关闭所有block的写入
     */
    /**
     * 中文：将当前可见的物理块标记为不活跃；不等待租约、不关闭执行器，也不替代上层写入准入控制。
     * English: Marks currently visible physical blocks inactive without waiting for leases or stopping executors; this does not replace upstream write admission control.
     */
    public void closeAllBlock() {
        freeBlocks.forEach(CacheBlockReferenceResource::setUnActive);
        keyBlockMap.forEach((key, block) -> block.setUnActive());
    }

    /**
     * 上传所有封口的Block
     */
    /**
     * 中文：遍历当前绑定尝试上传并等待已登记上传数归零；不负责封口或恢复，达到期限直接返回，中断则恢复标志并退出等待。
     * English: Attempts upload for current bindings and waits for registered uploads to drain; it neither seals nor recovers blocks, returns at the deadline, and restores interruption before ending the wait.
     * @param deadLine 基于 currentTimeMillis 的绝对截止时间，单位毫秒；English: absolute currentTimeMillis-based deadline in milliseconds
     */
    public void updateAllBlock(long deadLine) {
        for (Map.Entry<Long, CloudCacheBlock> entry : keyBlockMap.entrySet()) {
            // The weakly-consistent iterator may contain a just-recycled entry;
            // upload admission validates the captured metadata and binding atomically.
            // 中文：弱一致遍历可能看到已回收对象，稳定身份与上传准入由 updater 重新判断。
            // English: Weakly consistent iteration may expose recycled objects; the updater revalidates stable identity and admission.
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
    /**
     * 中文：返回既有绑定或等待空闲物理块后创建绑定；等待池容量时不持有 metadata 锁，返回值尚未持引用，调用者必须在对应锁下复核并获取租约。
     * English: Returns an existing binding or waits for a free block to create one; pool waits do not hold metadata monitors. The returned block is unpinned and must be revalidated and leased under the corresponding monitor.
     * @param fileFromOffset WAL 文件逻辑起始字节偏移；English: logical WAL file start in bytes
     * @param blockIndex WAL 内零基逻辑块序号；English: zero-based logical block index within the WAL
     * @param prefix 生成对象键的前缀；English: object-key prefix
     * @param defaultMappedFile 借用的 WAL 文件引用，正常上传路径必须有效；English: borrowed WAL file reference, required for normal upload
     * @return 尚未获取资源引用的物理块；English: physical block without an acquired resource reference
     * @throws InterruptedException 等待空闲池时被中断；English: interrupted while waiting for pool capacity
     * @throws IllegalStateException 管理器已关闭、逻辑任务已终结或池约束被破坏；English: manager closed, logical task terminal, or pool invariant violated
     */
    public CloudCacheBlock getBlock(long fileFromOffset, int blockIndex, String prefix, DefaultMappedFile defaultMappedFile) throws InterruptedException {
        long cacheBlockKey = ProjectUtil.buildBlockKey(fileFromOffset, blockIndex);
        BlockMetaData metadata = blockMetaDataManager.getOrCreate(fileFromOffset, blockIndex);
        while (!closed) {
            CloudCacheBlock existingBlock = keyBlockMap.get(cacheBlockKey);
            if (existingBlock != null) return existingBlock;
            // Never wait for pool capacity while holding a logical-block/stripe lock.
            // 中文：池等待必须在逻辑锁之外，否则持锁等待的写入者可能阻止归还容量。
            // English: Wait for pool capacity outside logical monitors, otherwise a waiter could block the recycling needed to provide capacity.
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
    /**
     * 中文：兼容入口，可能创建未绑定 WAL 对象的块；不获取租约，不能据此直接启动需要 WAL 引用的上传。
     * English: Compatibility entry that may allocate a block without a WAL object; it acquires no lease and does not by itself make the block suitable for upload requiring a WAL reference.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index within the file
     * @return 未持引用的块；English: unpinned block
     * @throws InterruptedException 等待空闲池时被中断；English: interrupted while waiting for the free pool
     */
    public CloudCacheBlock getBlock(long fileFromOffset, int blockIndex) throws InterruptedException {
        return getBlock(fileFromOffset, blockIndex, config.s3KeyPrefix, null);
    }


    /**
     * 中文：仅查询当前绑定快照，不分配资源或自动固定身份。
     * English: Looks up a binding snapshot without allocation or automatically pinning its identity.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     * @return 当前绑定或 null；English: current binding or null
     */
    public CloudCacheBlock getExistingBlock(long fileFromOffset, int blockIndex) {
        return keyBlockMap.get(ProjectUtil.buildBlockKey(fileFromOffset, blockIndex));
    }

    /**
     * 清除block并且放回到空闲池中
     *
     * @param block
     */
    /**
     * 中文：请求延迟清理：在捕获的 metadata 锁下复核身份并禁止写入；存在引用时等待最后一次 release 再回池。
     * English: Requests deferred cleanup by validating identity and disabling writes under the captured metadata monitor; outstanding references postpone recycling until their final release.
     * @param block 待清理块，null 或已无绑定时不操作；English: block to clean; null or unbound blocks are ignored
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

    /**
     * 中文：在 metadata 锁下校验身份、延迟清理标志及零引用，按键和值共同删除原映射后才回池；方法内部也会获取该锁。
     * English: Under metadata, validates identity, deferred cleanup, and zero references, removes the exact key/value binding, then returns the block to the pool; the method acquires the monitor itself.
     * @param block 待尝试回收的物理块；English: physical block to attempt recycling
     * @throws IllegalStateException 回池失败，可能违反唯一回收约束；English: returning to the pool failed, indicating a recycling invariant violation
     */
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
    /**
     * 中文：仅在损坏块已封口、WAL 复制完成、全部原始请求经过 core 且无人持引用时取得独占恢复租约；清零重放游标与完成字节，不在此读取 WAL。
     * English: Acquires an exclusive recovery lease only when the broken block is sealed, WAL copies are complete, all original requests have settled through core, and no references remain; resets replay accounting without reading WAL.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     * @return 携带一次租约的原绑定块，调用方必须恰好调用一次 finishRecovery；条件未齐则为 null；English: original bound block carrying one lease that requires exactly one finishRecovery call, or null when not ready
     */
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
            // 中文：恢复租约阻止普通请求追加与提前上传；expected/completed 相等确保原始请求已全部结算。
            // English: The recovery lease excludes ordinary appends and early upload; expected/completed equality establishes that original requests have all settled.
            metadata.setRecovering(true);
            metadata.clearFinishedBytes();
            block.resetWritePosition();
            block.setActive();
            block.getReference();
            return block;
        }
    }

    /**
     * 中文：结算恢复租约并复核 value 字节总数；成功清除损坏位并由最后一次 release 触发上传，失败标为终态并保留未确认 WAL，锁外派发失败通知。
     * English: Settles a recovery lease and verifies total value bytes; success clears broken state and permits final release to trigger upload, while failure becomes terminal and retains unacknowledged WAL before dispatching failure notifications outside the monitor.
     * @param block beginRecovery 返回且尚未归还的块；English: block with an outstanding lease returned by beginRecovery
     * @param success 重放过程是否成功，仍须通过字节数校验；English: replay outcome, additionally checked against byte totals
     * @throws IllegalStateException 没有正在持有的恢复租约；English: no active recovery lease
     */
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
            // 中文：release 可能回收并清空绑定，之后只用已捕获的 metadata，不再读取旧块身份。
            // English: Release may recycle and clear the binding; afterward use captured metadata rather than reading the former block identity.
            block.releaseReference();
        }
        if (!success) metadata.failAllFuture();
    }

    /**
     * 中文：轮询 core 写入数直到归零或截止；当前实现吞掉 sleep 异常，既不传播中断也不保证期限后所有写入已结束。
     * English: Polls core writer count until zero or the deadline; the current implementation swallows sleep exceptions, neither propagating interruption nor guaranteeing all writers have finished at timeout.
     * @param deadline 基于 currentTimeMillis 的绝对截止毫秒时间；English: absolute currentTimeMillis-based deadline in milliseconds
     */
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

    /**
     * 中文：停止并最多等待五秒上传队列消费线程；并不关闭上传执行器，也不停止上层恢复或写入线程。
     * English: Stops and joins the upload-queue worker for up to five seconds; does not stop the upload executor or upstream recovery/writer threads.
     * @throws IllegalStateException 等待被中断或消费线程未终止；English: join was interrupted or the worker did not terminate
     */
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
    /**
     * 中文：禁止继续分配并停止上传调度、等待上传执行器退出后关闭 Arena；调用方须先停稳写入与恢复，上传未退出则抛错保留内存，不等待用户通知回调；Arena 自身的关闭异常仅记录日志。
     * English: Stops allocation and upload dispatch, awaits upload termination, then closes the arena; callers must first quiesce writers and recovery. Upload termination failure preserves native memory, user callbacks are not awaited, and arena-close exceptions are only logged.
     */
    public void close() {
        closed = true;
        stopAllThread();
        // 中文：只有执行器确认终止，下面才能关闭其任务会读取的共享原生内存。
        // English: The executor must confirm termination before the shared native memory used by its tasks is closed.
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

    /**
     * 中文：读取物理块池的固定容量。
     * English: Reads the fixed physical pool capacity.
     * @return 物理块数量；English: number of physical blocks
     */
    public int getBlockCount() {
        return blockCount;
    }


    /**
     * 中文：读取当前空闲块数快照，不能用作随后分配成功的保证。
     * English: Reads a free-block count snapshot, not a guarantee that a subsequent allocation succeeds.
     * @return 当前队列内空闲块数；English: current number of queued free blocks
     */
    public int getFreeBlockCount() {
        return freeBlocks.size();
    }

    /**
     * 中文：读取所属实例名称。
     * English: Reads the owning instance name.
     * @return 实例名称；English: instance name
     */
    public String getInstanceName() {
        return instanceName;
    }

}
