package org.foreverfzl.cloudcache.wal.manager;

import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.datastruct.MetaInfo;
import org.foreverfzl.cloudcache.wal.storefile.AppendMessageResult;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;


/**
 * 这个类专门管理该Bucket存在的文件
 */
public class MappedFileManager {
    private static final Logger log = LoggerFactory.getLogger("MappedFileManager");
    public final String instanceName;
    public final String bucketName;

    // 3. 【核心骨架】：并发跳表。Key 是文件的起始 Offset，天生按位点升序排列
    private final ConcurrentSkipListMap<Long, DefaultMappedFile> mappedFiles = new ConcurrentSkipListMap<>();

    //该instance下所有的mappedFile的全局read指针，该指针之前的数据全部安全
    private volatile long globalReadPosition;
    //刷新所有文件的读指针
    private final Thread flushReadPositionThread;
    //检查所有的文件，控制文件是否要删除
    private final Thread chackMappedFileThread;
    //当前活跃文件的最后一个文件
    private final AtomicReference<DefaultMappedFile> activeMappedFile = new AtomicReference<>();
    //主要控制线程flushReadPosition、chackMappedFile等的执行
    private volatile boolean active = true;
    //该bucket的wal目录绝对地址
    private final String dirPath;
    //Bucket级别配置文件
    public final BucketConfig config;
    //WAL持久化文件地址
    private final String WAL_FILE_PATH;

    //文件水位线，活跃文件超过这个水位线会分配新的线程去创建新的文件
    public final long fileWaterMark;

    // Bucket元数据文件名字
    private static final String META_FILE_NAME = "bucketMeta";
    // bucket元数据文件大小4KB
    private static final long META_FILE_SIZE = 4 * 1024;

    //到达水位线 创建新文件的时候都会使用这个线程池
    private static final ExecutorService createNewFileExecutor = Executors.newSingleThreadExecutor();

    //该bucket对应的Block元数据管理者
    public BlockMetaDataManager blockMetaDataManager;


    public MappedFileManager(String dirPath, String instanceName, String bucketName, BucketConfig config) {
        this.instanceName = instanceName;
        this.bucketName = bucketName;
        this.dirPath = dirPath;
        this.config = config;
        this.WAL_FILE_PATH = dirPath + File.separator + "wal";
        this.fileWaterMark = (long) (config.walFileSize * 0.7);
        this.blockMetaDataManager = new BlockMetaDataManager();
        saveBucketMeta(new MetaInfo(config.s3KeyPrefix));//保存该bucket的元数据
        flushReadPositionThread = new Thread(new Runnable() {
            @Override
            public void run() {
                flushReadPositionTask();
            }
        });
        chackMappedFileThread = new Thread(new Runnable() {
            @Override
            public void run() {
                chackMappedFileTask();
            }
        });
        init();
    }

    private void init() {
//        flushReadPositionThread.start();
//        chackMappedFileThread.start();
        //刚开始的时候一个文件也没有，因此我们必须初始化一个文件
        synCreateMappedFile(0L);
        activeMappedFile.compareAndSet(null, mappedFiles.get(0L));
    }

    //todo 将数据添加到指定文件，如果文件满了则获取新的文件进行写，如果是其它错误则分情况而论
    public AppendMessageResult appendData(final DataStruct dataStruct) {
        AppendMessageResult result = null;
        DefaultMappedFile oldMappedFile = null;
        try {
            //先保存当前数据的文件
            oldMappedFile = activeMappedFile.get();
            oldMappedFile.hold();
            //先去目前活跃的文件中添加数据
            result = oldMappedFile.appendData(dataStruct);
            AppendMessageResult.AppendStatus status = result.getStatus();
            //文件到达结尾了或者该文件关闭了，使用新的文件重试,其它情况直接返回,交给Instance处理
            if (status == AppendMessageResult.AppendStatus.END_OF_FILE || status == AppendMessageResult.AppendStatus.FILE_CLOSED) {
                //获取最新的写文件
                long nextFileOffset = oldMappedFile.fileFromOffset + oldMappedFile.fileSize;
                synCreateMappedFile(nextFileOffset);
                DefaultMappedFile newFile = mappedFiles.get(nextFileOffset);
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
     * 同步创建新的文件并放入到容器中
     */
    private void synCreateMappedFile(long fileFromOffset) {
        String fileName = String.valueOf(fileFromOffset);
        //这里水位线线程 和 其它线程可能出现冲突，同时创建文件，需要加锁
        synchronized (fileName.intern()) {
            try {
                //先去看看新文件是否已经创建好了
                if (mappedFiles.get(fileFromOffset) != null) {
                    return;
                }
                DefaultMappedFile newFile = null;
                newFile = DefaultMappedFile.createFile(WAL_FILE_PATH, fileName, fileFromOffset, config.walFileSize,
                        config.blockSize, config.isWarmWalFile, config.isLockMappedFilePageCache, this);
                //文件为null有可能其它线程已经创建了文件了
                if (newFile == null) {
                    return;
                }
                mappedFiles.put(fileFromOffset, newFile);
            } catch (Exception e) {
                log.warn("Exception is{} . synCreateMappedFile failed, instance={},bucket={},fileName={}",
                        e, instanceName, bucketName, fileName);
                throw e;
            }

        }
    }

    private void flushReadPositionTask() {
        try {
            while (active) {
                //利用 floorEntry 解决绝对位点路由问题，
                //todo 这里可以用activeMappedFile优化，不优化也可以，时间复杂度为logn，但是数据量少可以忽略
                Map.Entry<Long, DefaultMappedFile> entry = mappedFiles.floorEntry(globalReadPosition);
                if (entry == null) {
                    // 如果实在没找到，说明系统还没初始化好第一个文件，sleep 等待
                    log.warn("flushReadPositionTask failed: can not find `{}` DefaultMappedFile Object", globalReadPosition);
                    Thread.sleep(config.pageFlushLevel);
                    continue;
                }
                DefaultMappedFile mappedFile = entry.getValue();
                long readPosition = mappedFile.readPosition;
                long wrotePosition = mappedFile.wrotePosition;
                if (readPosition != wrotePosition) {
                    //读取文件数据，检查数据是否正常，正常则force()更新readPosition
                    long size = wrotePosition - readPosition;
                    MemorySegment targetSegment = mappedFile.getMappedMemorySegment().asSlice(readPosition, size);
                    //强制刷盘
                    targetSegment.force();
                    //刷盘成功更新指针
                    mappedFile.readPosition = wrotePosition;
                    globalReadPosition += size;
                } else {
                    //readPosition == wrotePosition也有可能文件不能写入了
                    //不能写入原因之一 就是一条数据添加到文件中发现位置不够，因此将本文件设置为不可写入，然后用新的文件写入
                    if (!mappedFile.isAvailable()) {
                        globalReadPosition = mappedFile.fileFromOffset + mappedFile.fileSize;
                    }
                }
                Thread.sleep(config.pageFlushLevel);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    //保存该bucket的元数据
    private void saveBucketMeta(MetaInfo metaInfo) {
        Path path = Path.of(META_FILE_NAME);
        try (Arena arena = Arena.ofConfined();
             FileChannel fileChannel = FileChannel.open(
                     path,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.TRUNCATE_EXISTING)) {

            fileChannel.truncate(META_FILE_SIZE);

            MemorySegment segment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    META_FILE_SIZE,
                    arena
            );

            long pos = 0;

            // Magic
            segment.set(ValueLayout.JAVA_INT_UNALIGNED, pos, MetaInfo.MAGIC);
            pos += Integer.BYTES;

            // crc
            segment.set(ValueLayout.JAVA_INT_UNALIGNED, pos, metaInfo.getCrc());
            pos += Integer.BYTES;

            // dataLen
            segment.set(ValueLayout.JAVA_INT_UNALIGNED, pos, metaInfo.getDataLen());
            pos += Integer.BYTES;

            //data
            byte[] prefixBytes = metaInfo.getData();
            MemorySegment.copy(
                    prefixBytes,
                    0,
                    segment,
                    ValueLayout.JAVA_BYTE,
                    pos,
                    prefixBytes.length
            );
            // 强制刷盘
            segment.force();
        } catch (Exception e) {
            log.warn("bucket meta file create failed");
        }
    }


    //检查WAL文件的生命周期
    private void chackMappedFileTask() {

    }

    public AtomicReference<DefaultMappedFile> getActiveMappedFile() {
        return activeMappedFile;
    }

    //todo
    public void close() {
        createNewFileExecutor.shutdown();
    }
}
