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


public class S3CloudCacheInstance extends AbstractCloudCacheInstance {
    private static final Logger log = LoggerFactory.getLogger(LogName.CLOUD_CACHE_INSTANCE);
    /**
     * 名字一定要唯一并且不要更改
     */
    protected final String instanceName;

    /**
     * 全局配置文件
     */
    private final S3CloudCacheConfig config;
    /**
     * 持久化WAL文件管理者
     */
    private final WalInstanceBucketManager walInstanceBucketManager;
    /**
     * Cache的管理者
     */
    private final CoreInstanceBucketManager coreInstanceBucketManager;

    /**
     * 管理维护该Instance下所有的BucketWriter
     */
    private final ConcurrentHashMap<String, BucketWriterWriter> bucketWriters = new ConcurrentHashMap<>();

    private final S3Client s3Client;
    private final InstanceDirectoryLock directoryLock;

    /**
     * 全局数据恢复异步任务句柄
     */
    private volatile CompletableFuture<Void> recoveryFuture;
    private volatile Throwable recoveryFailure;
    private final Object lifecycleLock = new Object();
    private boolean started;
    private boolean closing;
    private boolean closed;


    public S3CloudCacheInstance(S3Client s3Client, S3CloudCacheConfig config) {
        this.s3Client = s3Client;
        // 先完整校验并深拷贝，禁止外部修改配置改变已运行实例的 WAL/Block 布局。
        this.config = java.util.Objects.requireNonNull(config, "config").snapshotAndValidate();
        this.config.walPath = (this.config.walPath != null ? this.config.walPath : ProjectUtil.USER_HOME)
                + ProjectUtil.WAL_FILE_ADDRESS;
        this.instanceName = this.config.instanceName;
        // 必须早于恢复、预创建文件和后台线程：同目录只能有一个拥有者（包括不同进程）。
        this.directoryLock = InstanceDirectoryLock.acquire(Paths.get(this.config.walPath, instanceName));
        WalInstanceBucketManager initializedWal = null;
        try {
            initializedWal = new WalInstanceBucketManager(instanceName, this.config);
            coreInstanceBucketManager = new CoreInstanceBucketManager(instanceName, s3Client, this.config);
            walInstanceBucketManager = initializedWal;
        } catch (RuntimeException | Error failure) {
            // 构造失败时对象不会交给用户，不能依赖用户调用 close 释放锁和线程。
            if (initializedWal != null) {
                try { initializedWal.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            }
            try { directoryLock.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /**
     * 获取BucketName对应的Bucket操作句柄
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
     *
     * @param bucketName 目标 Bucket 名称
     * @param s3Key      目标对象 Key
     * @param data       待上传数据，可为堆内或堆外 MemorySegment
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
    @Override
    public void start() {
        synchronized (lifecycleLock) {
            if (closing) throw new CloudCacheException("Instance is closing");
            if (started) return;
            started = true;
            startRecovery();
        }
    }

    private void startRecovery() {
        Path instancePath = Paths.get(config.walPath, instanceName);
        if (!Files.exists(instancePath)) {
            this.recoveryFuture = CompletableFuture.completedFuture(null);
            return;
        }
        try (Stream<Path> list = Files.list(instancePath)) {
            List<Path> bucketPathList = list.filter(Files::isDirectory).toList();
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
        if (isDirty == 0) {
            return;
        }
        int oldblockSize = bucketMetaInfo.getBlockSize();
        long oldFileSize = bucketMetaInfo.getFileSize();
        String oldPrefix = new String(bucketMetaInfo.getData(), StandardCharsets.UTF_8);
        // 未提交 WAL 按原布局恢复，不能先以新配置重写 bucketMeta 或扩缩文件。
        if (bucketConfig.blockSize != oldblockSize || bucketConfig.walFileSize != oldFileSize
                || !java.util.Objects.equals(bucketConfig.s3KeyPrefix, oldPrefix)) {
            throw new CloudCacheException("Recover outstanding WAL with its original block/file size and prefix first: " + path);
        }
        /*
         * 2.扫描获取wal目录下的所有文件的绝对路径，如果没有wal则默认不数据恢复
         */
        Path walPath = path.resolve("wal");
        if (!Files.exists(walPath) || !Files.isDirectory(walPath)) {
            return;
        }
        //获取所有的文件绝对地址
        try (Stream<Path> stream = Files.list(walPath)) {
            //根据文件名字进行排序
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
                    if (file.isBlockUploaded(index)) continue;
                    long blockEnd = (long) (index + 1) * oldBlockSize;
                    List<WalBlockReader.Record> records;
                    try {
                        records = WalBlockReader.readBlock(file, index);
                    } catch (Exception e) {
                        // 即便检查点滞后，也不能把一个损坏但非空的 WAL 当作空文件删除。
                        file.wrotePosition = Math.max(file.wrotePosition, blockEnd);
                        throw e;
                    }
                    if (records.isEmpty()) {
                        if ((long) index * oldBlockSize < file.readPosition) {
                            throw new CloudCacheException("Durable WAL Block is empty: " + path + ":" + index);
                        }
                        continue;
                    }
                    // 也扫描检查点之后的合法记录，数据已写入而文件头尚未更新时仍可恢复。
                    file.wrotePosition = Math.max(file.wrotePosition, blockEnd);
                    BlockMetaDataManager metadata = fileManager.blockMetaDataManager;
                    BlockMetaData meta = metadata.getOrCreate(fileOffset, index);
                    try {
                        for (WalBlockReader.Record record : records) {
                            int length = record.value().length;
                            metadata.addExpectedBytes(fileOffset, index, length);
                            metadata.addPageCacheBytes(fileOffset, index, length);
                            var result = blockManager.appendData(new HeapBlockDataStruct(file, index,
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
                file.close();
                file.release();
            }
        }
    }


    private long getFileFromOffset(Path path) {
        return Long.parseLong(path.getFileName().toString());
    }

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
            if (recoveryFuture != null) {
                try {
                    recoveryFuture.get(walWriteWaitTime + upLoadWaitTime, TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    log.error("Recovery failed; uncommitted WAL will be retained", e.getCause());
                }
            }
            long writeDeadline = System.currentTimeMillis() + walWriteWaitTime + blockWriteWaitTime;
            for (BucketWriterWriter writer : writers) writer.awaitWrites(writeDeadline);
            walInstanceBucketManager.stopScheduler();
            for (BucketWriterWriter writer : writers) {
                MappedFileManager wal = writer.getMappedManager();
                wal.stopAllThread();
                wal.closeAllFile();
                // 统一封口同时更新 PageCache-ready，失败上传的尾块仍必须持久化。
                wal.sealAllBlocks();
            }
            long uploadDeadline = System.currentTimeMillis() + upLoadWaitTime;
            for (BucketWriterWriter writer : writers) {
                CacheBlockManager core = coreInstanceBucketManager.onlyGetBlockManager(writer.getBucketName());
                writer.awaitRecovery(uploadDeadline);
                core.updateAllBlock(uploadDeadline);
            }
            // 在途恢复和上传结束/取消后，才能做最终检查点并卸载 WAL。
            for (BucketWriterWriter writer : writers) writer.close();
            coreInstanceBucketManager.close();
            for (BucketWriterWriter writer : writers) {
                MappedFileManager wal = writer.getMappedManager();
                wal.blockMetaDataManager.failUncommittedFutures();
                wal.endFlushFileReadPosition();
                wal.endMetaFlush();
                wal.endChackMappedFile();
                // 恢复失败时可能还有尚未注册到 manager 的旧文件，不能据空集合标记干净。
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
    private static final class InstanceDirectoryLock implements AutoCloseable {
        private final FileChannel channel;
        private final FileLock lock;

        private InstanceDirectoryLock(FileChannel channel, FileLock lock) {
            this.channel = channel;
            this.lock = lock;
        }

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
