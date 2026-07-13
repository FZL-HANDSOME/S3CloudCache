package org.foreverfzl.cloudcache.wal.manager;

import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.datastruct.MetaInfo;
import org.foreverfzl.cloudcache.wal.storefile.AppendMessageResult;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.exception.WalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
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

    //所有文件的上传指针
    private final int flushFileUpLoadPositonTime;
    //该线程就是刷新所有文件的上传指针
    private final Thread flushFileUpLoadPositionThread;
    //该instance下所有的mappedFile的全局上传指针，该指针之前的数据全上传到云上，该指针之前的文件可以删除
    private volatile long globalUpLoadPosition;
    //当前上传指针更新到的最后一个文件
    private final AtomicReference<DefaultMappedFile> upLoadActiveMappedFile = new AtomicReference<>();

    //检查所有的文件，控制文件是否要删除，把globalUpLoadPosition指针之前的文件全部删除，因为前面的文件已经上传到服务了
    private final int chackMappedFileTime;
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

    private final int flushFileMetaTime;
    //刷新文件元数据的线程，将文件的各个信息写入到对应文件的开头4KB
    private Thread fileMetaFlushThread;


    public MappedFileManager(String dirPath, String instanceName, String bucketName, BucketConfig config) {
        this.instanceName = instanceName;
        this.bucketName = bucketName;
        this.dirPath = dirPath;
        this.config = config;
        this.WAL_FILE_PATH = dirPath + File.separator + "wal";
        this.fileWaterMark = (long) (config.walFileSize * 0.7);
        this.blockMetaDataManager = new BlockMetaDataManager();
        this.saveBucketMeta(new MetaInfo(config.s3KeyPrefix));//保存该bucket的元数据
        this.flushFileMetaTime = config.flushFileMetaInfoTime;
        this.chackMappedFileTime = config.chackMappedFileTime;
        this.flushFileUpLoadPositonTime = config.flushFileUpLoadPositionTime;
        this.flushFileUpLoadPositionThread = new Thread(this::flushUpLoadPositionTask);
        this.chackMappedFileThread = new Thread(this::chackMappedFileTask);
        this.fileMetaFlushThread = new Thread(this::flushFileMeta);
        init();
    }

    //启动线程，创建初始文件等
    private void init() {
//        flushFileUpLoadPositionThread.start();
//        chackMappedFileThread.start();
//        fileMetaFlushThread.start();
        //刚开始的时候一个文件也没有，因此我们必须初始化一个文件
        synCreateMappedFile(0L);
        activeMappedFile.compareAndSet(null, mappedFiles.get(0L));
        upLoadActiveMappedFile.compareAndSet(null, mappedFiles.get(0L));
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
            //只要写没成功，关闭文件创建新的文件重试
            if (!result.isOk()) {
                oldMappedFile.close();
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

    //刷新全局文件的上传指针
    private void flushUpLoadPositionTask() {
        while (active) {
            try {
                DefaultMappedFile mappedFile = upLoadActiveMappedFile.get();
                if (mappedFile == null) {
                    // 如果实在没找到，说明系统还没初始化好第一个文件，sleep 等待
                    log.warn("flushUpLoadPositionTask failed: can not find DefaultMappedFile Object");
                    Thread.sleep(flushFileUpLoadPositonTime);
                    continue;
                }
                long curUpLoadPosition = mappedFile.upLoadPosition + mappedFile.fileFromOffset;
                if (curUpLoadPosition != globalUpLoadPosition) {
                    //如果不相等则更新
                    globalUpLoadPosition = curUpLoadPosition;
                } else {
                    //相等看看该文件是否还有可能更新上传指针
                    if (!mappedFile.isContinueUpdateUpLoadPosition()) {
                        Map.Entry<Long, DefaultMappedFile> next = mappedFiles.higherEntry(mappedFile.fileFromOffset);
                        if (next != null) {
                            globalUpLoadPosition = next.getKey();
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("flushUpLoadPositionTask failed", e);
            }
            try {
                Thread.sleep(flushFileUpLoadPositonTime);
            } catch (Exception e) {
                Thread.currentThread().interrupt();
                break;
            }

        }
    }

    //保存该bucket的元数据
    private void saveBucketMeta(MetaInfo metaInfo) {
        Path metaPath = Path.of(dirPath, META_FILE_NAME);
        try (Arena arena = Arena.ofConfined();
             FileChannel fileChannel = FileChannel.open(
                     metaPath,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.TRUNCATE_EXISTING)) {
            // 创建父目录
            Files.createDirectories(metaPath.getParent());
            // 固定文件大小
            fileChannel.truncate(META_FILE_SIZE);
            MemorySegment segment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    META_FILE_SIZE,
                    arena
            );
            long pos = 0;
            // CRC
            segment.set(ValueLayout.JAVA_INT_UNALIGNED, pos, metaInfo.getCrc());
            pos += Integer.BYTES;
            // dataLen
            segment.set(ValueLayout.JAVA_INT_UNALIGNED, pos, metaInfo.getDataLen());
            pos += Integer.BYTES;
            // data
            MemorySegment.copy(
                    metaInfo.getData(),
                    0,
                    segment,
                    ValueLayout.JAVA_BYTE,
                    pos,
                    metaInfo.getDataLen()
            );
            // 强制刷盘
            segment.force();
        } catch (Exception e) {
            log.warn("saveBucketMeta method failed,create bucketMeta failed", e);
        }
    }


    //检查WAL文件的生命周期，
    private void chackMappedFileTask() {
        while (active) {
            try {
                //获取全局上传指针之前的所有文件
                Collection<DefaultMappedFile> values = mappedFiles.headMap(globalUpLoadPosition).values();
                if (!values.isEmpty()) {
                    for (DefaultMappedFile file : values) {
                        if (file.canDelete()) {
                            //如果文件可以删除则直接删除
                            file.delete();
                            continue;
                        }
                        if (file.canClean()) {
                            //文件关闭了，但是资源没清除，清除资源下一次删除
                            file.clean();
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("chackMappedFileTask failed", e);
            }
            try {
                Thread.sleep(chackMappedFileTime);
            } catch (Exception e) {
                Thread.currentThread().interrupt();
                break;
            }

        }
    }


    private void flushFileMeta() {
        while (active) {
            for (Map.Entry<Long, DefaultMappedFile> entry : mappedFiles.entrySet()) {
                DefaultMappedFile mappedFile = entry.getValue();
                if (!mappedFile.metaDirty) {
                    //如果不是脏数据则直接跳过
                    continue;
                }
                try {
                    long readPos = mappedFile.readPosition;
                    long uploadPos = mappedFile.upLoadPosition;
                    // Use file creation time if needed; fall back to current time.
                    long updateTime = System.currentTimeMillis();
                    // Write into the first 4KB of the mapped file.
                    MemorySegment metaSegment = mappedFile.getMappedMemorySegment().asSlice(0, DefaultMappedFile.FILE_META_SIZE);
                    long pos = 0;
                    metaSegment.set(ValueLayout.JAVA_LONG, pos, readPos);
                    pos += Long.BYTES;
                    metaSegment.set(ValueLayout.JAVA_LONG, pos, uploadPos);
                    pos += Long.BYTES;
                    metaSegment.set(ValueLayout.JAVA_LONG, pos, updateTime);
                    // Ensure durability.
                    metaSegment.force();
                } catch (Exception e) {
                    log.warn("flushFileMeta: failed to write meta for {}file offset ", mappedFile.getFileName(), e);
                }
            }
            try {
                Thread.sleep(flushFileMetaTime);
            } catch (Exception e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public AtomicReference<DefaultMappedFile> getActiveMappedFile() {
        return activeMappedFile;
    }

    //todo
    public void close() {
        createNewFileExecutor.shutdown();
    }
}
