package org.foreverfzl.cloudcache.wal.manager;

import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.storefile.AppendMessageResult;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.io.RandomAccessFile;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
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

    //前缀文件的引用
    private final File prefixFile;
    // 前缀文件 文件名
    private static final String PREFIX_FILE_NAME = "prefix";
    // 前缀文件大小
    private static final long PREFIX_FILE_SIZE = 1024;

    //到达水位线 创建新文件的时候都会使用这个线程池
    private static final ExecutorService createNewFileExecutor = Executors.newSingleThreadExecutor();

    //该bucket对应的Block元数据管理者
    public BlockMetaDataManager blockMetaDataManager;


    public MappedFileManager(String dirPath, String instanceName, String bucketName, BucketConfig config) {
        this.instanceName = instanceName;
        this.bucketName = bucketName;
        this.dirPath = dirPath;
        this.config = config;
        this.prefixFile = saveBucketPrefix(config.s3KeyPrefix); //保存前缀
        this.WAL_FILE_PATH = dirPath + File.separator + "wal";
        this.fileWaterMark = (long) (config.walFileSize * 0.7);
        this.blockMetaDataManager=new BlockMetaDataManager();
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
            if(oldMappedFile!=null)oldMappedFile.release();
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


    /**
     * 创建一个 1MB 的文件并持久化用户自定义前缀
     * * @param prefix 用户自定义的 S3Key 前缀
     * 地址举例（C:\Users\root\ClouCache\store\instance1\textBucket\prefix）
     *
     * @return 成功创建并完成刷盘的文件引用 (File)
     */
    private File saveBucketPrefix(String prefix) {
        // 1. 校验并确保全局静态变量指定的目录结构存在
        File directory = new File(dirPath);
        if (!directory.exists() && !directory.mkdirs()) {
            throw new RuntimeException("无法创建目标元数据目录: " + dirPath);
        }
        // 定位具体的元数据配置文件引用
        File metaFile = new File(directory, PREFIX_FILE_NAME);
        // 2. 将字符串转换为统一的 UTF-8 字节数组
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        int prefixLen = prefixBytes.length;
        // 3. 安全边界校验：4字节长度字段 + 实际数据长度 不能超过 1MB 限制
        if (4 + prefixLen > PREFIX_FILE_SIZE) {
            throw new IllegalArgumentException("Prefix too long,Exceeds 1024 bytes");
        }
        // 4. 使用 rw 模式打开文件通道，确保具备读写权限
        try (RandomAccessFile raf = new RandomAccessFile(metaFile, "rw");
             FileChannel channel = raf.getChannel()) {
            raf.setLength(PREFIX_FILE_SIZE);
            // 5. 根据你的 PrefixDataStruct 协议布局构建堆内缓冲区
            // 内存布局：[ 4字节的 prefixLen ] + [ 变长的 prefix 字节数据 ]
            ByteBuffer buffer = ByteBuffer.allocate(4 + prefixLen);
            buffer.putInt(prefixLen);
            buffer.put(prefixBytes);
            // 切换为读模式，准备向通道外传输数据
            buffer.flip();
            // 6. 强制将物理指针归零，从文件头部开始覆盖写入
            channel.position(0);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            // 强制将 OS PageCache 中的元数据与数据强行同步刷入物理磁盘介质
            // 参数为 true 代表连同文件系统的 inode 时间戳等元数据一并刷盘，防御断电灾难
            channel.force(true);
        } catch (Exception e) {
            throw new RuntimeException("底层元数据文件 [bucket.meta] 持久化失败", e);
        }
        // 8. 返回该文件的引用
        return metaFile;
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
