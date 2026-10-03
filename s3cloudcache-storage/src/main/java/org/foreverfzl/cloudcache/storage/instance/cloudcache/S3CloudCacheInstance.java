package org.foreverfzl.cloudcache.storage.instance.cloudcache;


import org.foreverfzl.cloudcache.core.cache.CloudCacheBlock;
import org.foreverfzl.cloudcache.core.datastruct.HeapBlockDataStruct;
import org.foreverfzl.cloudcache.core.global.CoreInstanceBucketManager;
import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.storage.instance.bucket.BucketWriterWriter;
import org.foreverfzl.cloudcache.storage.instance.bucket.WalBlockReader;
import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.wal.Util.BucketMetaInfoUtil;
import org.foreverfzl.cloudcache.wal.Util.FileMetaInfoUtil;
import org.foreverfzl.cloudcache.wal.datastruct.BucketMetaInfo;
import org.foreverfzl.cloudcache.wal.datastruct.FileMetaInfo;
import org.foreverfzl.cloudcache.wal.global.WalInstanceBucketManager;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.foreverfzl.cloudchache.common.exception.CloudCacheException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.lang.foreign.MemorySegment;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Stream;


/**
 * 中文：对外生命周期入口，协调各 Bucket 的 WAL、堆外池、恢复与同步 S3 客户端；提交单位是 Block 而非单条记录。
 * English: Public lifecycle coordinator for bucket WALs, native pools, recovery and synchronous S3; the commit unit is a block, not a record.
 * 中文：构造取得本地目录独占权；start/首次获取 Writer 准备异步恢复，close 排空依赖后卸载内存。
 * English: Construction acquires local-directory ownership; start/first writer access schedules recovery, and close drains dependencies before unmapping.
 * 中文：未确认数据可重排，已确认块不得覆盖；本地目录锁不保证跨机器 S3 Key 唯一性。
 * English: Unacknowledged bytes may reorder, acknowledged blocks must not be overwritten; local locking does not ensure cross-machine S3 key uniqueness.
 */
public class S3CloudCacheInstance extends AbstractCloudCacheInstance {
    /** 中文：实例恢复/关闭诊断，不记录凭证；English: instance recovery/shutdown diagnostics, not credentials. */
    private static final Logger log = LoggerFactory.getLogger(LogName.CLOUD_CACHE_INSTANCE);
    /**
     * 名字一定要唯一并且不要更改
     * English: Stable local/object namespace component; do not change it while reusing WAL.
     */
    protected final String instanceName;

    /**
     * 全局配置文件
     * 中文：构造时取得的深快照，只修改内部 WAL 路径；English: deep construction-time snapshot; only its internal WAL path is adjusted.
     */
    private final S3CloudCacheConfig config;
    /**
     * 持久化WAL文件管理者
     * English: Owns bucket WAL managers and this instance's idle-sealing scheduler.
     */
    private final WalInstanceBucketManager walInstanceBucketManager;
    /**
     * Cache的管理者
     * English: Owns native pools/upload executors; must stop before WAL mappings are closed.
     */
    private final CoreInstanceBucketManager coreInstanceBucketManager;

    /**
     * 管理维护该Instance下所有的BucketWriter
     * 中文：含恢复创建的 Writer；创建与关闭快照由 lifecycleLock 协调。
     * English: Includes recovery-created writers; lifecycleLock coordinates creation with shutdown snapshots.
     */
    private final ConcurrentHashMap<String, BucketWriterWriter> bucketWriters = new ConcurrentHashMap<>();

    /** 中文：实例正常关闭时也关闭此客户端；正常使用需非 null，但构造器未强制校验。
     * English: Client also closed by normal instance shutdown; normal use needs non-null although construction does not enforce it. */
    private final S3Client s3Client;
    /** 中文：从构造到安全关闭持有的目录锁，超时不可提前释放；English: directory lease held from construction through safe shutdown, including timeout failures. */
    private final InstanceDirectoryLock directoryLock;

    /**
     * 全局数据恢复异步任务句柄
     * 中文：表示 WAL 重建结束，不等价于全部 S3 上传结束；English: tracks WAL reconstruction, not completion of all S3 uploads.
     */
    private volatile CompletableFuture<Void> recoveryFuture;
    /** 中文：可见的恢复失败标记，阻止后续 Writer 获取与错误 clean 标记；English: visible failure preventing later writer access and false clean marking. */
    private volatile Throwable recoveryFailure;
    /** 中文：保护生命周期布尔字段与 Writer 创建，不在网络调用期间持有；English: guards lifecycle flags/writer creation, not network calls. */
    private final Object lifecycleLock = new Object();
    /** 中文：已尝试启动，失败不自动重扫；English: startup attempted; failure does not automatically rescan. */
    private boolean started;
    /** 中文：只进不退的关闭准入标志；English: one-way admission-closure flag. */
    private boolean closing;
    /** 中文：已走到最终资源收尾阶段；English: final resource cleanup has reached its terminal phase. */
    private boolean closed;


    /**
     * 中文：先校验快照，再创建目录/锁和 Manager；历史 WAL 扫描延迟到 start，构造失败尽力回收已创建资源。
     * English: Validates/snapshots before directory locking and manager creation; WAL scanning waits for start; failed construction cleans initialized resources.
     * @param s3Client 中文：实例负责最终关闭的同步客户端，勿跨独立实例共享；English: synchronous client eventually closed here; do not share across independent instances.
     * @param config 中文：非 null 可变配置，深复制后不跟踪外部修改；English: non-null mutable config, deep-copied without tracking later changes.
     * @throws IllegalArgumentException 中文：配置或路径非法；English: invalid configuration/path.
     * @throws CloudCacheException 中文：目录创建或独占锁获取失败；English: directory creation or exclusive-lock acquisition fails.
     */
    public S3CloudCacheInstance(S3Client s3Client, S3CloudCacheConfig config) {
        this.s3Client = s3Client;
        // 先完整校验并深拷贝，禁止外部修改配置改变已运行实例的 WAL/Block 布局。
        // English: Snapshot before allocating resources so caller setters cannot alter a running instance's layout.
        this.config = java.util.Objects.requireNonNull(config, "config").snapshotAndValidate();
        this.config.walPath = (this.config.walPath != null ? this.config.walPath : ProjectUtil.USER_HOME)
                + ProjectUtil.WAL_FILE_ADDRESS;
        this.instanceName = this.config.instanceName;
        // 必须早于恢复、预创建文件和后台线程：同目录只能有一个拥有者（包括不同进程）。
        // English: Lock before recovery, precreation and background activity, including across processes.
        this.directoryLock = InstanceDirectoryLock.acquire(Paths.get(this.config.walPath, instanceName));
        WalInstanceBucketManager initializedWal = null;
        try {
            initializedWal = new WalInstanceBucketManager(instanceName, this.config);
            coreInstanceBucketManager = new CoreInstanceBucketManager(instanceName, s3Client, this.config);
            walInstanceBucketManager = initializedWal;
        } catch (RuntimeException | Error failure) {
            // 构造失败时对象不会交给用户，不能依赖用户调用 close 释放锁和线程。
            // English: An unsuccessful constructor exposes no object on which the caller could invoke close.
            if (initializedWal != null) {
                try { initializedWal.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            }
            try { directoryLock.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /**
     * 获取BucketName对应的Bucket操作句柄
     * 中文：原子取得唯一 Writer；首次调用启动恢复但不等待所有恢复上传完成。
     * English: Atomically obtains one writer; first access starts recovery without waiting for all recovery/uploads.
     * @param bucketName 中文：安全路径组件；远端 Bucket 需另行创建；English: safe path component; provision the remote bucket separately.
     * @return 中文：实例共享的 Writer，其 Manager 仍归实例所有；English: shared writer whose managers remain instance-owned.
     * @throws CloudCacheException 中文：关闭中或已发现恢复失败；English: shutting down or recovery failure already known.
     * @throws IllegalArgumentException 中文：Bucket 路径名称非法；English: invalid bucket path component.
     */
    public BucketWriterWriter getBucketWriterInstance(String bucketName) {
        S3CloudCacheConfig.validatePathComponent(bucketName, "bucketName");
        synchronized (lifecycleLock) {
            if (closing) throw new CloudCacheException("Instance is closing");
            if (!started) start();
            if (recoveryFailure != null || (recoveryFuture != null && recoveryFuture.isCompletedExceptionally())) {
                throw new CloudCacheException("WAL recovery failed; repair the retained WAL before accepting new writes");
            }
            return bucketWriters.computeIfAbsent(bucketName, name -> {
                MappedFileManager wal = walInstanceBucketManager.getOrCreateBucketFileManager(name);
                CacheBlockManager core = coreInstanceBucketManager.getOrCreateBlockManager(name, wal.blockMetaDataManager);
                return new BucketWriterWriter(name, wal, core, wal.blockMetaDataManager, this);
            });
        }
    }

    /**
     * 使用原始 S3Client 将指定内存段上传为一个对象。
     * 中文：同步旁路，不写 WAL、不批量合并、不更新提交确认；人工恢复成功后需另外显式 ack。
     * English: Synchronous bypass without WAL, batching or commit acknowledgment; manual recovery must explicitly acknowledge afterward.
     * 中文：上传全部 byteSize；段在调用期间须存活且可由当前线程访问，asByteBuffer 最大长度为 Integer.MAX_VALUE。
     * English: Uploads full byteSize; keep the segment alive/thread-accessible during the call; asByteBuffer permits at most Integer.MAX_VALUE bytes.
     * 中文：不参加 Writer 准入或上传计数，不能与实例 close 并发；调用方自行避免覆盖已确认 Key。
     * English: Bypasses writer admission/upload accounting; do not race instance close, and avoid overwriting acknowledged keys yourself.
     *
     * @param bucketName 目标 Bucket 名称；English: nonblank destination bucket.
     * @param s3Key      目标对象 Key；English: nonblank destination key.
     * @param data       待上传数据，可为堆内或堆外 MemorySegment；English: non-null caller-owned heap/native segment, not closed here.
     * @return 中文：SDK PUT 响应，不代表 WAL 已确认；English: SDK PUT response, not WAL acknowledgment.
     * @throws CloudCacheException 中文：名称空白或数据 null；English: blank names or null data.
     * @throws RuntimeException 中文：段访问、大小限制或 SDK 上传失败；English: segment access/size or SDK upload failure.
     */
    public PutObjectResponse s3RawPutObject(String bucketName, String s3Key, MemorySegment data) {
        if (bucketName == null || bucketName.isBlank()) {
            throw new CloudCacheException("bucketName can not be null or blank");
        }
        if (s3Key == null || s3Key.isBlank()) {
            throw new CloudCacheException("s3Key can not be null or blank");
        }
        if (data == null) {
            throw new CloudCacheException("data can not be null");
        }

        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(s3Key).build();
        PutObjectResponse response = s3Client.putObject(request, RequestBody.fromByteBuffer(data.asByteBuffer()));
        return response;
    }

    //启动数据恢复
    /**
     * 中文：幂等准备恢复任务；返回只表示已安排任务，恢复异常记录在聚合 Future 中。
     * English: Idempotently schedules recovery; return only means scheduling, with failures retained in the aggregate future.
     * @throws CloudCacheException 中文：已开始关闭；English: shutdown has begun.
     */
    @Override
    public void start() {
        synchronized (lifecycleLock) {
            if (closing) throw new CloudCacheException("Instance is closing");
            if (started) return;
            started = true;
            startRecovery();
        }
    }

    /**
     * 中文：持 lifecycleLock 扫描 Bucket，使用有限平台线程池重建；关闭目录流并在任务终结后关闭恢复池。
     * English: Scans buckets under lifecycleLock, reconstructs on a bounded platform pool, and closes directory streams/pool when finished.
     * 中文：保存异常而非伪装成功；上传完成由 Core 另行管理。
     * English: Retains errors instead of fabricating success; Core separately manages upload completion.
     */
    private void startRecovery() {
        Path instancePath = Paths.get(config.walPath, instanceName);
        if (!Files.exists(instancePath)) {
            this.recoveryFuture = CompletableFuture.completedFuture(null);
            return;
        }
        try (Stream<Path> list = Files.list(instancePath)) {
            List<Path> bucketPathList = list.filter(Files::isDirectory).toList();
            // 中文：过滤普通文件如 .instance.lock；并行度不超过 Bucket 数与 CPU 数两倍中的较小值。
            // English: Exclude regular files such as .instance.lock; concurrency is capped by bucket count and twice CPU count.
            int bucketCount = bucketPathList.size();
            if (bucketCount == 0) {
                this.recoveryFuture = CompletableFuture.completedFuture(null);
                return;
            }
            int threadCount = Math.min(bucketCount, Runtime.getRuntime().availableProcessors() * 2);
            ExecutorService recoverExecutorService = Executors.newFixedThreadPool(threadCount);
            List<CompletableFuture<Void>> futures = new ArrayList<>(bucketCount);
            for (Path path : bucketPathList) {
                try {
                    String bucketName = path.getFileName().toString();
                    BucketConfig bucketConfig = config.getBucketConfig(bucketName);
                    this.chackDirectoryAndFile(path, bucketName, bucketConfig, recoverExecutorService, futures);
                } catch (Exception e) {
                    recoveryFailure = e;
                    log.error("Recover preparation failed for path: {}", path, e);
                    futures.add(CompletableFuture.failedFuture(e));
                }
            }
            if (futures.isEmpty()) {
                this.recoveryFuture = CompletableFuture.completedFuture(null);
                recoverExecutorService.shutdown();
                return;
            }
            // 绑定异步终结回调：当所有子任务全部完成（无论成功或失败）后，自动关闭线程池
            // start() 方法不会在此处阻塞，并将全局句柄赋值给 recoveryFuture
            // English: allOf covers all buckets, preserves failures and shuts the recovery pool without blocking here.
            this.recoveryFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            log.error("Data recovery completed exceptionally.", throwable);
                        } else {
                            log.info("All buckets recovery completed, shutting down recoverExecutorService.");
                        }
                        recoverExecutorService.shutdown();
                    });
        } catch (Exception e) {
            recoveryFailure = e;
            log.error("Start async recovery failed for instance: {}", instanceName, e);
            this.recoveryFuture = CompletableFuture.failedFuture(e);
        }
    }

    //恢复一个具体bucket中的数据
    /**
     * 中文：同步校验旧布局、枚举文件和登记上下文，再异步重建此 Bucket；不先用新配置覆盖遗留数据。
     * English: Validates persisted layout, enumerates files and registers context before asynchronous bucket replay, without overwriting old configuration first.
     * @param path 中文：已有 Bucket 目录；English: existing bucket directory.
     * @param bucketName 中文：目录对应名称；English: bucket name represented by the directory.
     * @param bucketConfig 中文：恢复时须匹配旧布局/前缀的快照；English: snapshot required to match persisted layout/prefix during recovery.
     * @param recoverExecutorService 中文：由 startRecovery 关闭的池；English: pool closed by startRecovery.
     * @param futures 中文：当前调用线程拥有的聚合任务列表；English: aggregate task list owned by the calling thread.
     * @throws CloudCacheException 中文：目录、布局或枚举失败；损坏元数据异常也向上传递；English: directory/layout/enumeration failure; corrupt metadata errors also propagate.
     */
    private void chackDirectoryAndFile(Path path, String bucketName, BucketConfig bucketConfig,
                                       ExecutorService recoverExecutorService,
                                       List<CompletableFuture<Void>> futures) {
        if (path == null || !Files.exists(path)) {
            throw new CloudCacheException("bucket directory not exists: " + path);
        }
        if (!Files.isDirectory(path)) {
            throw new CloudCacheException("path is not directory: " + path);
        }
        /*
         * 1.读取bucketMeta，如果没有该文件则默认代表不需要数据恢复
         * 中文：更正：缺元数据且无 WAL 才是空状态；有 WAL 却缺元数据必须拒绝。
         * English: Clarification: absent metadata is empty only without WAL; WAL without metadata must fail safely.
         */
        BucketMetaInfo bucketMetaInfo = BucketMetaInfoUtil.readBucketMetaFile(path);
        if (bucketMetaInfo == null) {
            Path existingWal = path.resolve("wal");
            if (Files.isDirectory(existingWal)) {
                try (Stream<Path> files = Files.list(existingWal)) {
                    if (files.anyMatch(Files::isRegularFile)) {
                        throw new CloudCacheException("WAL exists but bucketMeta is missing or invalid: " + path);
                    }
                } catch (java.io.IOException e) {
                    throw new CloudCacheException("Cannot inspect outstanding WAL: " + path, e);
                }
            }
            return;
        }
        int isDirty = bucketMetaInfo.getIsDirty();
        //isDirty为0说明没可恢复的数据
        // 中文：当前实现信任 dirty=0 并跳过扫描；它不是独立的目录完整性证明。
        // English: The implementation trusts dirty=0 and skips scanning; this is not an independent directory-integrity proof.
        if (isDirty == 0) {
            return;
        }
        int oldblockSize = bucketMetaInfo.getBlockSize();
        long oldFileSize = bucketMetaInfo.getFileSize();
        String oldPrefix = new String(bucketMetaInfo.getData(), StandardCharsets.UTF_8);
        // 未提交 WAL 按原布局恢复，不能先以新配置重写 bucketMeta 或扩缩文件。
        // English: Preserve persisted block/file sizes and prefix before interpreting uncommitted bytes.
        if (bucketConfig.blockSize != oldblockSize || bucketConfig.walFileSize != oldFileSize
                || !java.util.Objects.equals(bucketConfig.s3KeyPrefix, oldPrefix)) {
            throw new CloudCacheException("Recover outstanding WAL with its original block/file size and prefix first: " + path);
        }
        /*
         * 2.扫描获取wal目录下的所有文件的绝对路径，如果没有wal则默认不数据恢复
         * English: Enumerate existing regular WAL files; absence of the directory/files leaves nothing to replay.
         */
        Path walPath = path.resolve("wal");
        if (!Files.exists(walPath) || !Files.isDirectory(walPath)) {
            return;
        }
        //获取所有的文件绝对地址
        try (Stream<Path> stream = Files.list(walPath)) {
            //根据文件名字进行排序
            // 中文：文件名是十进制逻辑文件偏移，不是任意用户文件名；新文件从旧末尾之后开始，并受持久编号约束。
            // English: Names encode decimal logical file offsets, not arbitrary user files; new allocation starts after old files and respects the durable counter.
            List<Path> walFiles = stream.filter(Files::isRegularFile).sorted(Comparator.comparingLong(this::getFileFromOffset)).toList();
            //没有文件也不进行数据恢复
            if (walFiles.isEmpty()) {
                return;
            }
            long endFileFromOffset = getFileFromOffset(walFiles.getLast()) + FileMetaInfo.FILE_META_SIZE + oldFileSize;
            //创建该bucket对应manager
            MappedFileManager fileManager = walInstanceBucketManager.getOrCreateBucketFileManager(bucketName, endFileFromOffset);
            CacheBlockManager blockManager = coreInstanceBucketManager.getOrCreateBlockManager(bucketName, fileManager.blockMetaDataManager);
            // 恢复创建的 Bucket 也必须参与关闭和运行时 Block 恢复。
            // English: Register recovery-created writers so shutdown and runtime broken-block recovery include them.
            bucketWriters.computeIfAbsent(bucketName, name -> new BucketWriterWriter(name, fileManager,
                    blockManager, fileManager.blockMetaDataManager, this));
            //恢复wal目录下的数据
            futures.add(recoverFile(walFiles, fileManager, blockManager, oldFileSize, oldblockSize, oldPrefix, recoverExecutorService));
        } catch (Exception e) {
            log.error("path={} recover failed", path, e);
            throw new CloudCacheException("Cannot prepare WAL recovery: " + path, e);
        }
    }

    //恢复一个bucket中的文件
    /**
     * 中文：异步边界设在文件重建层；任务异常保持异常完成，不允许 allOf 误报成功。
     * English: Places the async boundary at file reconstruction; task errors stay exceptional so allOf cannot report false success.
     * @param walFiles 中文：按偏移排序的旧文件列表，提交后不再修改；English: sorted old-file list, unchanged after scheduling.
     * @param fileManager 中文：接管恢复映射的管理器；English: manager taking ownership of replay mappings.
     * @param recoverBlockManager 中文：用于重建纯 Value 的物理池；English: native pool rebuilding Value bytes.
     * @param oldFileSize 中文：旧 WAL 数据区字节数，不含文件头；English: old WAL data bytes excluding the file header.
     * @param oldblockSize 中文：旧逻辑 Block 字节数；English: old logical block size in bytes.
     * @param prefix 中文：旧对象 Key 前缀；English: persisted object-key prefix.
     * @param recoverExecutorService 中文：外部拥有的执行器；English: externally owned executor.
     * @return 中文：重建完成 Future，并非全部远端 PUT 完成；English: reconstruction future, not completion of every remote PUT.
     */
    private CompletableFuture<Void> recoverFile(List<Path> walFiles, MappedFileManager fileManager,
                                                CacheBlockManager recoverBlockManager, long oldFileSize,
                                                int oldblockSize, String prefix,
                                                ExecutorService recoverExecutorService) {
        return CompletableFuture.runAsync(() -> {
            try {
                doRecoverFile(walFiles, fileManager, recoverBlockManager, oldFileSize, oldblockSize, prefix);
            } catch (Exception e) {
                recoveryFailure = e;
                throw e;
            }
        }, recoverExecutorService);
    }

    /**
     * 中文：按文件顺序扫描，每块完整验证后才重建；已确认块跳过，未确认块可重新排列且没有跨进程旧 Future 可通知。
     * English: Scans files in order and rebuilds only fully validated blocks; acknowledged blocks are skipped and unacknowledged blocks may reorder without old-process futures.
     * 中文：任何损坏使当前任务失败并保留 WAL，不把合法前缀当成功整块；每个文件在 finally 关闭写入准入并释放本次引用。
     * English: Corruption fails the task while retaining WAL, never committing a valid prefix; finally closes file admission and releases this replay reference.
     * @param walFiles 中文：有序 WAL 路径；English: ordered WAL paths.
     * @param fileManager 中文：注册并最终关闭映射的拥有者；English: owner registering and eventually closing mappings.
     * @param blockManager 中文：接收重建数据的 Core Manager；English: Core manager receiving replayed data.
     * @param oldFileSize 中文：每文件数据区字节数；English: data-area bytes per file.
     * @param oldBlockSize 中文：每逻辑块字节数；English: bytes per logical block.
     * @param prefix 中文：用于恢复对象身份的原前缀；English: original prefix used to reconstruct object identity.
     * @throws RuntimeException 中文：解析、恢复位置或 Core 重建失败；English: parse, position restoration or Core reconstruction failure.
     */
    private void doRecoverFile(List<Path> walFiles, MappedFileManager fileManager,
                               CacheBlockManager blockManager, long oldFileSize, int oldBlockSize, String prefix) {
        for (Path path : walFiles) {
            long fileOffset = getFileFromOffset(path);
            DefaultMappedFile file = new DefaultMappedFile(path.getParent().toString(), path.getFileName().toString(),
                    fileOffset, oldFileSize, path.toFile(), oldBlockSize, false, false, fileManager);
            file.hold();
            try {
                FileMetaInfo info = FileMetaInfoUtil.getFileMetaInfo(file);
                file.restorePositions(info.getReadPosition(), info.getUploadPosition());
                fileManager.addMappedFile(file);
                int blockCount = Math.toIntExact(oldFileSize / oldBlockSize);
                for (int index = 0; index < blockCount; index++) {
                    // 已确认 Block 的布局已交给用户，不得按 WAL 顺序重新覆盖。
                    // English: Confirmed layouts have been published; WAL order must not overwrite their existing S3 offsets.
                    if (file.isBlockUploaded(index)) continue;
                    long blockEnd = (long) (index + 1) * oldBlockSize;
                    List<WalBlockReader.Record> records;
                    try {
                        records = WalBlockReader.readBlock(file, index);
                    } catch (Exception e) {
                        // 即便检查点滞后，也不能把一个损坏但非空的 WAL 当作空文件删除。
                        // English: Conservatively mark usage despite stale checkpoints so corrupt nonempty WAL cannot be deleted as empty.
                        file.wrotePosition = Math.max(file.wrotePosition, blockEnd);
                        throw e;
                    }
                    if (records.isEmpty()) {
                        // 中文：已声明落盘的范围不应全空；检查点之外的预创建零块可正常跳过。
                        // English: A declared durable range cannot be empty; zero precreated blocks beyond the checkpoint may be skipped.
                        if ((long) index * oldBlockSize < file.readPosition) {
                            throw new CloudCacheException("Durable WAL Block is empty: " + path + ":" + index);
                        }
                        continue;
                    }
                    // 也扫描检查点之后的合法记录，数据已写入而文件头尚未更新时仍可恢复。
                    // English: Scan valid records beyond persisted checkpoints to recover bytes written before header refresh.
                    file.wrotePosition = Math.max(file.wrotePosition, blockEnd);
                    BlockMetaDataManager metadata = fileManager.blockMetaDataManager;
                    BlockMetaData meta = metadata.getOrCreate(fileOffset, index);
                    try {
                        for (WalBlockReader.Record record : records) {
                            int length = record.value().length;
                            metadata.addExpectedBytes(fileOffset, index, length);
                            metadata.addPageCacheBytes(fileOffset, index, length);
                            var result = blockManager.appendData(new HeapBlockDataStruct(file, index,
                                    // 中文：启动恢复没有原 Future，false 避免登记普通请求完成计数；不改变记录 Value。
                                    // English: Startup has no old future; false avoids ordinary-request completion accounting and leaves Value bytes unchanged.
                                    record.value(), 0, length), prefix, null, false);
                            if (!result.result()) throw new CloudCacheException("Cannot rebuild WAL Block: " + path + ":" + index);
                        }
                        metadata.trySeal(fileOffset, index);
                        file.setBlockStateArrayFinishedPageCache(index);
                        blockManager.updateBlock(blockManager.getExistingBlock(fileOffset, index));
                    } catch (Exception e) {
                        meta.markUploadFailed();
                        synchronized (meta) {
                            CloudCacheBlock block = blockManager.getExistingBlock(fileOffset, index);
                            if (block != null && block.getBlockMetaData() == meta) {
                                block.getReference();
                                block.setDelayClean();
                                block.releaseReference();
                            }
                        }
                        throw e;
                    }
                }
            } catch (Exception e) {
                log.error("WAL recovery failed; original file retained: {}", path, e);
                throw e;
            } finally {
                // 中文：file.close 仅关闭追加准入；真正卸载/删除仍由 Manager 的引用和确认条件决定。
                // English: file.close closes append admission only; manager reference/ack conditions govern actual unmapping/deletion.
                file.close();
                file.release();
            }
        }
    }


    /**
     * 中文：解码数字文件名；目录中的非数字文件会使恢复失败，不静默忽略可能的数据。
     * English: Decodes a numeric filename; nonnumeric files fail recovery rather than silently hiding possible data.
     * @param path 中文：WAL 文件路径；English: WAL file path.
     * @return 中文：包含历史文件头步长的逻辑偏移，字节；English: logical byte offset including prior file-header strides.
     * @throws NumberFormatException 中文：文件名不是有效 long；English: filename is not a valid long.
     */
    private long getFileFromOffset(Path path) {
        return Long.parseLong(path.getFileName().toString());
    }

    /**
     * 中文：串行关闭实例：关准入→等待启动恢复/调用→停维护→封口→恢复/上传收尾→最终 force/检查点→卸载→关闭客户端/目录锁。
     * English: Serial shutdown: close admission, drain startup/calls, stop maintenance, seal, drain recovery/uploads, force/checkpoint, unmap, close client/lock.
     * 中文：等待阶段以异常退出或上传池最终仍有活跃线程时保留尚在使用的资源；上传阶段截止可转入额外的执行器等待，并非立即抛错。
     * English: Failed waits or an executor still alive after final termination waits retain in-use resources; an upload deadline can lead to additional executor waits rather than immediate failure.
     * 中文：关闭失败后保持 closing，可在处理原因后重试；底层 join 可能使总耗时超过预算之和。
     * English: Failed shutdown keeps closing state for retry after resolving the cause; lower-level joins may exceed the sum of budgets.
     * 中文：不等待用户 Future 回调执行结束；失败块留在 WAL，不能把正常 close 返回等同于所有请求成功。
     * English: Does not await user future callbacks; failed blocks remain in WAL, so normal close return does not imply every request succeeded.
     * @param walWriteWaitTime 中文：非负毫秒，参与启动恢复及写入等待预算；English: nonnegative milliseconds contributing to startup/write waits.
     * @param blockWriteWaitTime 中文：非负毫秒，与 WAL 预算组成共享写入截止时间；English: nonnegative milliseconds combined into the shared write deadline.
     * @param upLoadWaitTime 中文：非负毫秒，运行时恢复和上传共享截止时间；English: nonnegative milliseconds shared by runtime recovery/uploads.
     * @throws IllegalArgumentException 中文：任一预算为负；English: any budget is negative.
     * @throws CloudCacheException 中文：等待中断或启动恢复超时；English: interrupted wait or startup recovery timeout.
     * @throws RuntimeException 中文：底层关闭或等待失败，可能保留资源；English: lower-level shutdown/wait failure, potentially retaining resources.
     */
    @Override
    public synchronized void close(long walWriteWaitTime, long blockWriteWaitTime, long upLoadWaitTime) {
        if (walWriteWaitTime < 0 || blockWriteWaitTime < 0 || upLoadWaitTime < 0) {
            throw new IllegalArgumentException("Close timeouts must be nonnegative");
        }
        List<BucketWriterWriter> writers;
        synchronized (lifecycleLock) {
            if (closed) return;
            closing = true;
            writers = List.copyOf(bucketWriters.values());
            writers.forEach(BucketWriterWriter::beginClosing);
        }
        try {
            // 超时不卸载仍在使用的内存；保持关闭准入，调用方可重试 close。
            // English: Keep ownership/admission closed on timeout; callers may retry close after live work finishes.
            if (recoveryFuture != null) {
                try {
                    recoveryFuture.get(walWriteWaitTime + upLoadWaitTime, TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    log.error("Recovery failed; uncommitted WAL will be retained", e.getCause());
                }
            }
            long writeDeadline = System.currentTimeMillis() + walWriteWaitTime + blockWriteWaitTime;
            // 中文：所有 Bucket 共享阶段截止时间，不能每个 Bucket 重新获得完整等待预算。
            // English: Buckets share this phase deadline rather than each receiving a fresh full budget.
            for (BucketWriterWriter writer : writers) writer.awaitWrites(writeDeadline);
            walInstanceBucketManager.stopScheduler();
            for (BucketWriterWriter writer : writers) {
                MappedFileManager wal = writer.getMappedManager();
                wal.stopAllThread();
                wal.closeAllFile();
                // 统一封口同时更新 PageCache-ready，失败上传的尾块仍必须持久化。
                // English: Sealing also publishes WAL readiness; failed-upload tails still need persistence.
                wal.sealAllBlocks();
            }
            long uploadDeadline = System.currentTimeMillis() + upLoadWaitTime;
            for (BucketWriterWriter writer : writers) {
                CacheBlockManager core = coreInstanceBucketManager.onlyGetBlockManager(writer.getBucketName());
                writer.awaitRecovery(uploadDeadline);
                core.updateAllBlock(uploadDeadline);
            }
            // 在途恢复和上传结束/取消后，才能做最终检查点并卸载 WAL。
            // English: Finish/cancel recovery and uploads before final checkpoints and WAL unmapping.
            for (BucketWriterWriter writer : writers) writer.close();
            coreInstanceBucketManager.close();
            for (BucketWriterWriter writer : writers) {
                MappedFileManager wal = writer.getMappedManager();
                wal.blockMetaDataManager.failUncommittedFutures();
                wal.endFlushFileReadPosition();
                wal.endMetaFlush();
                wal.endChackMappedFile();
                // 恢复失败时可能还有尚未注册到 manager 的旧文件，不能据空集合标记干净。
                // English: A failed scan may leave unregistered old files, so an empty manager alone cannot prove a clean bucket.
                if (wal.mappedFileIsEmpty() && recoveryFailure == null) {
                    BucketMetaInfoUtil.updateIsDirty(0, wal.getBucketMetaFileSegment());
                }
            }
            walInstanceBucketManager.close();
            try {
                if (s3Client != null) s3Client.close();
            } finally {
                // 到这里存储任务和映射已退出，即使客户端 close 抛错也不能永久占住 WAL 目录。
                // 此前等待写入/恢复/上传超时不会进入此分支，仍保留独占权供调用方重试。
                // English: Release ownership even if client.close fails only after storage is safe; earlier wait failures never reach this branch.
                try {
                    directoryLock.close();
                } finally {
                    synchronized (lifecycleLock) {
                        closed = true;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CloudCacheException("Close interrupted; resources retained for retry", e);
        } catch (TimeoutException e) {
            throw new CloudCacheException("Recovery still running; resources retained for retry", e);
        }
    }

    /** 锁文件永久保留，只释放 OS 锁；删除文件会让新旧进程锁住不同的文件对象。 */
    /**
     * 中文：本地目录资源所有权封装；锁文件不是 WAL，也不是需要删除的临时文件。
     * English: Local-directory ownership wrapper; the persistent lock file is neither WAL nor a disposable temporary file.
     */
    private static final class InstanceDirectoryLock implements AutoCloseable {
        /** 中文：必须保持打开以维持锁，关闭时总是释放；English: kept open to retain the lock and always closed during cleanup. */
        private final FileChannel channel;
        /** 中文：操作系统独占锁，不负责跨机器对象身份；English: OS exclusive lock, not cross-machine object identity. */
        private final FileLock lock;

        /**
         * 中文：只包装已经成功取得的资源；English: wraps resources already acquired successfully.
         * @param channel 中文：转移关闭责任的通道；English: channel whose close responsibility is transferred.
         * @param lock 中文：通道上的有效锁；English: valid lock on that channel.
         */
        private InstanceDirectoryLock(FileChannel channel, FileLock lock) {
            this.channel = channel;
            this.lock = lock;
        }

        /**
         * 中文：创建目录并立即尝试独占，不等待其他进程释放；任何失败都关闭已经打开的通道。
         * English: Creates the directory and attempts ownership without waiting for another process; closes an opened channel on failure.
         * @param directory 中文：已由实例名/路径配置确定的实例目录；English: instance directory resolved from validated name/configuration.
         * @return 中文：必须关闭的独占权对象；English: ownership object that must be closed.
         * @throws CloudCacheException 中文：IO 失败或锁已占用；English: I/O failure or already-held lock.
         */
        static InstanceDirectoryLock acquire(Path directory) {
            FileChannel channel = null;
            try {
                Files.createDirectories(directory);
                channel = FileChannel.open(directory.resolve(".instance.lock"),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock lock = channel.tryLock();
                if (lock == null) throw new CloudCacheException("WAL directory is already in use: " + directory);
                return new InstanceDirectoryLock(channel, lock);
            } catch (IOException | RuntimeException failure) {
                if (channel != null) {
                    try { channel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
                }
                throw new CloudCacheException("Cannot acquire exclusive WAL directory: " + directory, failure);
            }
        }

        /**
         * 中文：释放 OS 锁并关闭通道，不删除锁文件；仅在存储线程和映射都已退出后调用。
         * English: Releases the OS lock/closes the channel without deleting the file; call only after storage workers/mappings are finished.
         * @throws CloudCacheException 中文：锁或通道释放 IO 失败；English: lock/channel cleanup I/O failure.
         */
        @Override
        public void close() {
            try {
                try {
                    if (lock.isValid()) lock.release();
                } finally {
                    channel.close();
                }
            } catch (IOException e) {
                throw new CloudCacheException("Cannot release WAL directory lock", e);
            }
        }
    }


}
