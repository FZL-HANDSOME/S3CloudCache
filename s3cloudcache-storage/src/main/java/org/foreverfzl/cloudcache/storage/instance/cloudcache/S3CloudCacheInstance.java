package org.foreverfzl.cloudcache.storage.instance.cloudcache;


import org.foreverfzl.cloudcache.core.cache.CloudCacheBlock;
import org.foreverfzl.cloudcache.core.datastruct.HeapBlockDataStruct;
import org.foreverfzl.cloudcache.core.global.CoreInstanceBucketManager;
import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.storage.instance.bucket.BucketWriterWriter;
import org.foreverfzl.cloudcache.wal.Util.BucketMetaInfoUtil;
import org.foreverfzl.cloudcache.wal.Util.FileMetaInfoUtil;
import org.foreverfzl.cloudcache.wal.datastruct.BucketMetaInfo;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
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

import java.io.File;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import java.util.zip.CRC32;


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


    public S3CloudCacheInstance(S3Client s3Client, S3CloudCacheConfig config) {
        this.s3Client = s3Client;
        this.config = config;
        this.instanceName = config.instanceName;
        config.walPath = config.walPath != null ? config.walPath + ProjectUtil.WAL_FILE_ADDRESS : ProjectUtil.USER_HOME + ProjectUtil.WAL_FILE_ADDRESS;
        walInstanceBucketManager = new WalInstanceBucketManager(instanceName, config);
        coreInstanceBucketManager = new CoreInstanceBucketManager(instanceName, s3Client, config);
    }

    /**
     * 获取BucketName对应的Bucket操作句柄
     */
    public BucketWriterWriter getBucketWriterInstance(String bucketName) {
        if (bucketName == null || bucketName.isBlank()) {
            throw new CloudCacheException("bucketName can not null");
        }
        BucketWriterWriter bucketWriterWriter = bucketWriters.get(bucketName);
        if (bucketWriterWriter != null) return bucketWriterWriter;
        //去容器中看看有没有对应的manager，有则返回，没有则创建
        MappedFileManager bucketWalManager = walInstanceBucketManager.getOrCreateBucketFileManager(bucketName);
        CacheBlockManager bucketCoreManager = coreInstanceBucketManager.getOrCreateBlockManager(bucketName, bucketWalManager.blockMetaDataManager);
        bucketWriterWriter = new BucketWriterWriter(bucketName, bucketWalManager, bucketCoreManager, bucketWalManager.blockMetaDataManager, this);
        bucketWriters.put(bucketName, bucketWriterWriter);
        return bucketWriterWriter;
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
    public void start() {
        Path instancePath = Paths.get(config.walPath, instanceName);
        if (!Files.exists(instancePath)) {
            return;
        }

        try (Stream<Path> list = Files.list(instancePath)) {
            List<Path> bucketPathList = list.toList();
            int bucketCount = bucketPathList.size();
            if (bucketCount == 0) {
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
                    log.error("Recover preparation failed for path: {}", path, e);
                }
            }
            // 绑定异步终结回调：当所有子任务全部完成（无论成功或失败）后，自动关闭线程池
            // start() 方法不会在此处阻塞
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .whenComplete((unused, throwable) -> {
                        log.info("All buckets recovery completed, shutting down recoverExecutorService.");
                        recoverExecutorService.shutdown();
                    });

        } catch (Exception e) {
            log.error("Start async recovery failed for instance: {}", instanceName, e);
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
        //看看新的BlockSize是否比原来的BlockSize小，如果小的话就不恢复以前的数据
        //因为我们采用的是最新配置的CacheBlock，如果老配置大于新配置需要多阶段提交上传，这里简单化
        if (bucketConfig.blockSize < oldblockSize) {
            log.warn("The new block size is smaller than the old block size. file path is{}", path);
            return;
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
            long endFileFromOffset = getFileFromOffset(walFiles.getLast()) + oldFileSize;
            //创建该bucket对应manager
            MappedFileManager fileManager = walInstanceBucketManager.getOrCreateBucketFileManager(bucketName, endFileFromOffset);
            CacheBlockManager blockManager = coreInstanceBucketManager.getOrCreateBlockManager(bucketName, fileManager.blockMetaDataManager);
            //恢复wal目录下的数据
            futures.add(recoverFile(walFiles, fileManager, blockManager, oldFileSize, oldblockSize, oldPrefix,recoverExecutorService));
        } catch (Exception e) {

        }
    }

    //恢复一个bucket中的文件
    private CompletableFuture<Void> recoverFile(List<Path> walFiles, MappedFileManager fileManager,
                                                CacheBlockManager recoverBlockManager, long oldFileSize,
                                                int oldblockSize, String prefix,
                                                ExecutorService recoverExecutorService) {
        return CompletableFuture.runAsync(
                () -> doRecoverFile(walFiles, fileManager, recoverBlockManager, oldFileSize, oldblockSize, prefix),
                recoverExecutorService
        ).exceptionally(ex -> {
            log.error("Async WAL recovery failed", ex);
            return null;
        });
    }

    private void doRecoverFile(List<Path> walFiles, MappedFileManager fileManager, CacheBlockManager recoverBlockManager, long oldFileSize, int oldblockSize, String prefix) {
        CRC32 crc32 = new CRC32();
        //遍历所有的文件并且读取前4KB数据并创建文件，然后进行数据恢复
        long curPos = 0;
        long endPos = 0;
        for (Path curPath : walFiles) {
            String fileName = curPath.getFileName().toString();
            long fileFromOffset = Long.parseLong(fileName);
            File file = curPath.toFile();
            try {
                //创建该文件的Java对象
                DefaultMappedFile defaultMappedFile = new DefaultMappedFile(curPath.getParent().toString(), fileName, fileFromOffset, oldFileSize, file, oldblockSize, false, false, fileManager);
                //获取该文件对应的元数据
                FileMetaInfo fileMetaInfo = FileMetaInfoUtil.getFileMetaInfo(defaultMappedFile);
                if (fileMetaInfo == null) {
                    continue;
                }
                long readPosition = fileMetaInfo.getReadPosition();
                long upLoadPosition = fileMetaInfo.getUploadPosition();
                defaultMappedFile.wrotePosition = readPosition;
                defaultMappedFile.readPosition = readPosition;
                defaultMappedFile.upLoadPosition = upLoadPosition;
                //将该文件添加到manage中
                fileManager.addMappedFile(defaultMappedFile);
                //不需要恢复数据，将文件关闭，让后台线程自己检测删除
                if (readPosition == upLoadPosition) {
                    defaultMappedFile.close();
                    continue;
                }
                //需要数据恢复的文件，读取文件指定区域数据进行恢复
                curPos = upLoadPosition;
                int blockCount = (int) ProjectUtil.divideByPower((readPosition - upLoadPosition), oldblockSize);
                //获取元数据管路者
                BlockMetaDataManager blockMetaDataManager = fileManager.blockMetaDataManager;
                for (int i = 1; i <= blockCount; i++) {
                    endPos = curPos + oldblockSize;
                    int blockIndex = Math.toIntExact(ProjectUtil.divideByPower(curPos, oldblockSize));
                    //如果可以读int并且是正常数据则读取
                    //如果一个Block结束了会在结尾打上end标志，end标志占用4字节，如果结尾位置4字节都不够默认就是结束了
                    while (endPos - curPos >= 4 && defaultMappedFile.getInt(curPos) == DataStruct.MAGIC_NUMBER) {
                        curPos += 4;
                        int checksum = defaultMappedFile.getInt(curPos);
                        curPos += 4;
                        int valueLen = defaultMappedFile.getInt(curPos);
                        curPos += 4;
                        byte[] orgData = defaultMappedFile.getOrgData(curPos, valueLen);
                        crc32.update(orgData);
                        if ((int) crc32.getValue() != checksum) {
                            //数据错误直接退出
                            break;
                        }
                        blockMetaDataManager.addExpectedBytes(fileFromOffset, blockIndex, valueLen);
                        blockMetaDataManager.addPageCacheBytes(fileFromOffset, blockIndex, valueLen);
                        recoverBlockManager.appendData(new HeapBlockDataStruct(defaultMappedFile, fileFromOffset, blockIndex, orgData, 0, valueLen), prefix);
                        crc32.reset();
                    }
                    //尝试封口
                    blockMetaDataManager.trySeal(fileFromOffset, blockIndex);
                    //该block数据结束了，将该block上传
                    CloudCacheBlock block = recoverBlockManager.getExistingBlock(fileFromOffset, blockIndex);
                    recoverBlockManager.updateBlock(block);
                    curPos = upLoadPosition + ((long) i * oldblockSize);
                }
                //将该文件设置为清除
                defaultMappedFile.upLoadPosition = readPosition;
                defaultMappedFile.close();
            } catch (Exception e) {
                log.warn("recovering=>{} file created failed", fileFromOffset, e);
            }
        }
    }


    private long getFileFromOffset(Path path) {
        return Long.parseLong(path.getFileName().toString());
    }

    @Override
    public void close(long walWriteWaitTime, long upLoadWaitTime) {
        long writeDeadline = System.currentTimeMillis() + walWriteWaitTime;
        Collection<BucketWriterWriter> writers = bucketWriters.values();
        // 1. 利用 try-with-resources 自动管理虚拟线程池的生命周期
        try (ExecutorService closeExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (BucketWriterWriter writer : writers) {
//                closeExecutor.execute(() -> {
                try {
                    // 1：先将所有的 bucket 设置为关闭中，不可写入
                    writer.close();
                    String bucketName = writer.getBucketName();
                    CacheBlockManager cacheBlockManager = coreInstanceBucketManager.onlyGetBlockManager(bucketName);
                    MappedFileManager mappedFileManager = walInstanceBucketManager.onlyGetFileManager(bucketName);
                    // 2：限时等待该 writer 完成写入
                    mappedFileManager.waitWriterFinished(writeDeadline);
                    // 3：封口所有的 block
                    BlockMetaDataManager blockMetaDataManager = cacheBlockManager.blockMetaDataManager;
                    blockMetaDataManager.trySealAllBlock();
                    // 4：限时上传所有的 block
                    long upLoadDeadline = System.currentTimeMillis() + upLoadWaitTime;
                    cacheBlockManager.updateAllBlock(upLoadDeadline);
                    //5：刷新读指针
                    mappedFileManager.endFlushFileReadPosition();
                    //此时要真正的关闭文件、block，并且不允许文件指针进行改动，这样保证了6 7步骤的指针状态是一致的
                    mappedFileManager.closeAllFile();
                    cacheBlockManager.closeAllBlock();
                    // 6：强制刷新所有文件的元数据区域
                    mappedFileManager.endMetaFlush();
                    // 7：最后检查一遍文件，把能删除的文件删除
                    mappedFileManager.endChackMappedFile();
                    // 8：若没有剩余文件，修改 bucketMeta 文件的 isDirty 为 0
                    if (mappedFileManager.mappedFileIsEmpty()) {
                        MemorySegment bucketMetaFileSegment = mappedFileManager.getBucketMetaFileSegment();
                        BucketMetaInfoUtil.updateIsDirty(0, bucketMetaFileSegment);
                    }
                } catch (Throwable t) {
                    log.error("Failed to close bucket writer for bucket: {}", writer.getBucketName(), t);
                }
//                });
            }
        } //执行到这里时，Java 自动调用 closeExecutor.close()，主线程在此强行阻塞，直到所有虚拟线程全部执行完成

        // 所有 Bucket 虚拟线程彻底执行完毕后，安全关闭底层 Manager 全局资源
        try {
            walInstanceBucketManager.close();
        } catch (Exception e) {
            log.error("Failed to close walInstanceBucketManager", e);
        }

        try {
            coreInstanceBucketManager.close();
        } catch (Exception e) {
            log.error("Failed to close coreInstanceBucketManager", e);
        }
        //关闭S3client
        if (s3Client != null) s3Client.close();
    }


}
