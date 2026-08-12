package org.foreverfzl.cloudcache.wal.manager;

import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.wal.Util.BucketMetaInfoUtil;
import org.foreverfzl.cloudcache.wal.Util.FileMetaInfoUtil;
import org.foreverfzl.cloudcache.wal.datastruct.BucketMetaInfo;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.storefile.AppendMessageResult;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


/**
 * 这个类专门管理该Bucket存在的文件
 */
public class MappedFileManager {
    private static final Logger log = LoggerFactory.getLogger(LogName.MAPPED_FILE_MANAGER);
    public final String instanceName;
    public final String bucketName;

    // 3. 【核心骨架】：并发跳表。Key 是文件的起始 Offset，天生按位点升序排列
    private final ConcurrentSkipListMap<Long, DefaultMappedFile> mappedFiles = new ConcurrentSkipListMap<>();

    //检查所有的文件，控制文件是否要删除，把globalUpLoadPosition指针之前的文件全部删除，因为前面的文件已经上传到服务了
    private final int chackMappedFileTime;
    private final Thread chackMappedFileThread;
    private volatile boolean chackMappedFileThreadState = true;
    //当前活跃文件的最后一个文件
    private final AtomicReference<DefaultMappedFile> activeMappedFile = new AtomicReference<>();

    //该bucket的wal目录绝对地址
    private final String dirPath;
    //Bucket级别配置文件
    public final BucketConfig config;
    //WAL持久化文件地址
    private final String WAL_FILE_PATH;

    //文件水位线，活跃文件超过这个水位线会分配新的线程去创建新的文件
    public final long fileWaterMark;

    //到达水位线 创建新文件的时候都会使用这个线程池
    private final ExecutorService createNewFileExecutor = Executors.newSingleThreadExecutor();

    //该bucket对应的Block元数据管理者
    public BlockMetaDataManager blockMetaDataManager;

    private final int flushFileMetaTime;
    //刷新文件元数据的线程，将文件的各个信息写入到对应文件的开头4KB
    private final Thread fileMetaFlushThread;
    private volatile boolean fileMetaFlushThreadState = true;

    //刷新所有文件的读指针，默认5S
    private final int flushReadPositionTime = 5000;
    private final Thread flushFileReadPositionThread;
    private volatile boolean flushFileReadPositionThreadState = true;

    //bucket元数据mmp
    private Arena bucketMetaFileArena;
    private MemorySegment bucketMetaFileSegment;


    public MappedFileManager(String dirPath, String instanceName, String bucketName, BucketConfig config, long fromOffset) {
        this.instanceName = instanceName;
        this.bucketName = bucketName;
        this.dirPath = dirPath;
        this.config = config;
        this.WAL_FILE_PATH = dirPath + File.separator + "wal";
        this.fileWaterMark = (long) (config.walFileSize * 0.7);
        this.blockMetaDataManager = new BlockMetaDataManager();
        this.bucketMetaFileArena = Arena.ofShared();
        bucketMetaFileSegment = BucketMetaInfoUtil.createAndMapBucketMetaFile(new BucketMetaInfo(config.blockSize, config.walFileSize, config.s3KeyPrefix), Path.of(dirPath), bucketMetaFileArena);//保存该bucket的元数据
        this.chackMappedFileTime = config.chackMappedFileTime;
        this.chackMappedFileThread = new Thread(this::chackMappedFileTask);
        this.flushFileMetaTime = config.flushFileMetaInfoTime;
        this.fileMetaFlushThread = new Thread(this::flushFileMeta);
        this.flushFileReadPositionThread = new Thread(this::flushReadPositionTask);
        init(fromOffset);
    }


    //启动线程，创建初始文件等
    private void init(long fromOffset) {
        chackMappedFileThread.start();
        fileMetaFlushThread.start();
        flushFileReadPositionThread.start();
        //刚开始的时候一个文件也没有，因此我们必须初始化一个文件
        DefaultMappedFile startFile = synCreateMappedFile(fromOffset);
        activeMappedFile.compareAndSet(null, startFile);
    }


    public AppendMessageResult appendData(final DataStruct dataStruct) {
        AppendMessageResult result = null;
        DefaultMappedFile oldMappedFile = null;
        try {
            //先保存当前数据的文件
            oldMappedFile = activeMappedFile.get();
            oldMappedFile.hold();
            //先去目前活跃的文件中添加数据
            result = oldMappedFile.appendData(dataStruct);
            //如果是文件结尾或者关闭，则创建新的文件进行写
            AppendMessageResult.AppendStatus status = result.getStatus();
            if (status == AppendMessageResult.AppendStatus.END_OF_FILE
                    || status == AppendMessageResult.AppendStatus.FILE_CLOSED) {
                //获取最新的写文件
                long nextFileOffset = oldMappedFile.fileFromOffset + oldMappedFile.fileSize;
                DefaultMappedFile newFile = synCreateMappedFile(nextFileOffset);
                if (newFile != null) {
                    activeMappedFile.compareAndSet(oldMappedFile, newFile);
                    //然后使用新的文件进行写
                    oldMappedFile = activeMappedFile.get();
                    result = oldMappedFile.appendData(dataStruct);
                }
            }
        } finally {
            if (oldMappedFile != null) oldMappedFile.release();
        }
        return result;
    }


    /**
     * 当到达水位线70%创建一个:创建新文件的CompletableFuture任务，当DefaultMappedFile文件检测到水位线就会触发这个方法
     */
    public void tryCreateNextFileWhenReachFileWaterMark(long nextFileFromOffset) {
        createNewFileExecutor.execute(() -> {
            synCreateMappedFile(nextFileFromOffset);
        });
    }


    /**
     * 同步创建新的文件并放入到容器中（默认使用配置文件 创建文件）
     */
    public DefaultMappedFile synCreateMappedFile(long fileFromOffset) {
        return synCreateMappedFile(fileFromOffset, config.walFileSize, config.blockSize, config.isWarmWalFile, config.isLockMappedFilePageCache);
    }


    //自定义创建文件
    public DefaultMappedFile synCreateMappedFile(long fileFromOffset, long walFileSize, int blockSize, boolean isWarm, boolean isLock) {
        String fileName = String.valueOf(fileFromOffset);
        DefaultMappedFile defaultMappedFile = mappedFiles.get(fileFromOffset);
        if (defaultMappedFile != null) {
            return defaultMappedFile;
        }
        //这里水位线线程 和 其它线程可能出现冲突，同时创建文件，需要加锁
        synchronized (fileName.intern()) {
            try {
                //先去看看新文件是否已经创建好了
                defaultMappedFile = mappedFiles.get(fileFromOffset);
                if (defaultMappedFile != null) {
                    return defaultMappedFile;
                }
                DefaultMappedFile newFile = DefaultMappedFile.createFile(WAL_FILE_PATH, fileName, fileFromOffset, walFileSize,
                        blockSize, isWarm, isLock, this);
                mappedFiles.put(fileFromOffset, newFile);
                return newFile;
            } catch (Exception e) {
                log.warn("Exception is{} . synCreateMappedFile failed, instance={},bucket={},fileName={}",
                        e, instanceName, bucketName, fileName);
                throw e;
            }
        }
    }


    private void flushReadPositionTask() {
        while (flushFileReadPositionThreadState) {
            try {
                flushReadPositionTaskExtracted();
                Thread.sleep(flushReadPositionTime);
            } catch (Exception e) {
                flushFileReadPositionThreadState = false;
                break;
            }
        }
        flushReadPositionTaskExtracted();
    }

    private void flushReadPositionTaskExtracted() {
        try {
            Collection<DefaultMappedFile> values = mappedFiles.values();
            if (!values.isEmpty()) {
                for (DefaultMappedFile file : values) {
                    if (file.isCleanup()) continue;
                    file.ackReadPosition();
                }
            }
        } catch (Exception e) {
            log.warn("flushReadPositionTask failed", e);
        }
    }


    //检查WAL文件的生命周期，
    private void chackMappedFileTask() {
        while (chackMappedFileThreadState) {
            try {
                chackMappedFileTaskExtracted();
                Thread.sleep(chackMappedFileTime);
            } catch (InterruptedException e) {
                chackMappedFileThreadState = false;
                break;
            }
        }
        //被打断退出循环后再执行最后一次
        chackMappedFileTaskExtracted();
    }

    private void chackMappedFileTaskExtracted() {
        try {
            Collection<DefaultMappedFile> values = mappedFiles.values();
            if (!values.isEmpty()) {
                for (DefaultMappedFile file : values) {
                    if (file.canClean()) {
                        //清除资源
                        file.clean();
                        //删除文件
                        file.delete();
                    }
                }
            }
        } catch (Exception e) {
            log.warn("chackMappedFileTask failed", e);
        }
    }

    //刷新文件元数据区域(刷新的都是需要改变的数据)
    private void flushFileMeta() {
        while (fileMetaFlushThreadState) {
            try {
                flushFileMetaExtracted();
                Thread.sleep(flushFileMetaTime);
            } catch (Exception e) {
                fileMetaFlushThreadState = false;
                break;
            }
        }
        flushFileMetaExtracted();
    }

    private void flushFileMetaExtracted() {
        for (Map.Entry<Long, DefaultMappedFile> entry : mappedFiles.entrySet()) {
            DefaultMappedFile mappedFile = entry.getValue();
            if (DefaultMappedFile.DIRTY_UPDATER.get(mappedFile) == 0) {
                //如果不是脏数据则直接跳过
                continue;
            }
            //刷新元数据
            FileMetaInfoUtil.flushFileMetaInfo(mappedFile);
        }
    }

    public void addMappedFile(DefaultMappedFile mappedFile) {
        mappedFiles.put(mappedFile.fileFromOffset, mappedFile);
    }

    public void removeMappedFile(long fileFromOffset) {
        mappedFiles.remove(fileFromOffset);
    }

    public DefaultMappedFile getMappedFile(long fileFromOffset) {
        return mappedFiles.get(fileFromOffset);
    }

    public AtomicReference<DefaultMappedFile> getActiveMappedFile() {
        return activeMappedFile;
    }

    //只有项目关闭时才会调用
    public void closeAllFile() {
        mappedFiles.values().forEach((defaultMappedFile -> {
            //关闭文件
            defaultMappedFile.close();
            //禁止指针的更新
            defaultMappedFile.posActive = false;
        }));
    }

    //打断线程，让线程执行最后一次
    public void endFlushFileReadPosition() {
        flushFileReadPositionThread.interrupt();
    }

    public void endMetaFlush() {
        fileMetaFlushThread.interrupt();
    }

    public void endChackMappedFile() {
        chackMappedFileThread.interrupt();
    }


    /**
     * 仅关闭后台线程、线程池及堆外内存资源
     */
    public void close() {
        log.info("Closing MappedFileManager resources for instance: {}, bucket: {}", instanceName, bucketName);

        // 2. 释放跳表及 activeMappedFile 中的 DefaultMappedFile 内存引用与句柄
        closeMappedFiles();

        // 3. 卸载 Bucket 级 FFM (Foreign Function & Memory) 堆外内存区域
        closeBucketMetaArena();

        // 4. 关闭线程池
        closeThreadPool();

        log.info("MappedFileManager resources closed successfully for bucket: {}", bucketName);
    }


    /**
     * 第二步：关闭并清理所有 DefaultMappedFile 的内存与句柄
     */
    private void closeMappedFiles() {
        for (DefaultMappedFile mappedFile : mappedFiles.values()) {
            if (mappedFile != null) {
                try {
                    mappedFile.clean();
                } catch (Exception e) {
                    log.error("Failed to close MappedFile in bucket: {}", bucketName, e);
                }
            }
        }
        activeMappedFile.set(null);
        mappedFiles.clear();
    }

    /**
     * 第三步：释放 JDK 21+ FFM Arena 堆外物理内存
     */
    private void closeBucketMetaArena() {
        if (bucketMetaFileArena != null) {
            try {
                bucketMetaFileArena.close(); // 触发底层的 unmap，立即释放物理内存映射
                bucketMetaFileArena = null;
                bucketMetaFileSegment = null;
            } catch (Exception e) {
                log.error("Failed to close bucketMetaFileArena for bucket: {}", bucketName, e);
            }
        }
    }

    /**
     * 第四步：关闭文件创建线程池
     */
    private void closeThreadPool() {
        if (!createNewFileExecutor.isShutdown()) {
            createNewFileExecutor.shutdown();
            try {
                if (!createNewFileExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                    createNewFileExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                createNewFileExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean hasMappedFile() {
        return !mappedFiles.isEmpty();
    }

    public MemorySegment getBucketMetaFileSegment() {
        return bucketMetaFileSegment;
    }
}


