package org.foreverfzl.cloudcache.wal.manager;

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
import java.util.concurrent.locks.ReentrantLock;

/**
 * 这个类专门管理该Bucket存在的文件
 */
public class MappedFileManager implements AutoCloseable {
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
    private volatile DefaultMappedFile activeMappedFile;
    //主要控制线程flushReadPosition、chackMappedFile等的执行
    private volatile boolean active = true;
    //该bucket的wal目录绝对地址
    private final String dirPath;
    //Bucket级别配置文件
    private final BucketConfig config;
    //WAL持久化文件地址
    private final String WAL_FILE_PATH;

    //文件水位线，活跃文件超过这个水位线会分配新的线程去创建新的文件
    private final long fileWaterMark;

    //前缀文件的引用
    private final File prefixFile;
    // 前缀文件 文件名
    private static final String PREFIX_FILE_NAME = "prefix";
    // 前缀文件大小
    private static final long PREFIX_FILE_SIZE = 1024;

    ReentrantLock appendFailLock=new ReentrantLock();

    //到达水位线 创建新文件的时候都会使用这个 虚拟线程工厂
    private static final ExecutorService virtualThreadFactory= Executors.newVirtualThreadPerTaskExecutor();
    //当活跃的文件内容超过50%的时候会预生成一个新的 DefaultMappedFile，preAllocatedFuture就是用来保存预生成文件的结果
    private final AtomicReference<CompletableFuture<DefaultMappedFile>> preAllocatedFuture = new AtomicReference<>(null);

    public MappedFileManager(String dirPath, String instanceName, String bucketName, BucketConfig config) {
        this.instanceName = instanceName;
        this.bucketName = bucketName;
        this.dirPath = dirPath;
        this.config = config;
        this.prefixFile = saveBucketPrefix(config.s3KeyPrefix); //保存前缀
        this.WAL_FILE_PATH=dirPath+File.separator+"wal";
        this.fileWaterMark =(long)(config.walFileSize*0.7);
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
    }

    private void init() {
        flushReadPositionThread.start();
        chackMappedFileThread.start();
    }

    //todo 添加数据的方法
    public AppendMessageResult appendData(){
        return null;
    }

    /**
     * 获取或者创建文件，并更新activeMappedFile属性
     */
    private void getOrCreateNewActiveMappedFile(DefaultMappedFile oldMappedFile) {

        try {
            appendFailLock.lock();
            if(oldMappedFile!=activeMappedFile){
                return;
            }
            //切换新的写文件，或者创建新的文件
            long newFileFromOffset=oldMappedFile.getFileFromOffset()+config.walFileSize;
            DefaultMappedFile newMappedFile = mappedFiles.get(newFileFromOffset);
            if(newMappedFile!=null){
                //说明新文件以及准备好了
                this.activeMappedFile=newMappedFile;
                return;
            }
            //新文件没有准备好，先去看看是否有创建新文件的任务，如果有则等待获取结果
            CompletableFuture<DefaultMappedFile> future = preAllocatedFuture.get();
            if(future!=null){
                try {
                    // 限时等待后台线程池分配完
                    newMappedFile = future.get(3, TimeUnit.SECONDS);
                } catch (Exception e) {
                    // 万一线程池满了或者创建失败，当前写线程亲自同步创建兜底（自愈）
                    newMappedFile = synCreateMappedFile(newFileFromOffset);
                }
            }else {
                //新文件没有创建，该线程自己创建
                newMappedFile = synCreateMappedFile(newFileFromOffset);
            }
            this.activeMappedFile = newMappedFile;
        }finally {
            appendFailLock.unlock();
        }

    }

    /**
     * 当到达水位线70%创建一个:创建新文件的CompletableFuture任务
     */
    private void tryCreateNextFileWhenReachFileWaterMark(long nextFileFromOffset){
        if(preAllocatedFuture.get()!=null){
            return;
        }

        CompletableFuture<DefaultMappedFile> future = new CompletableFuture<>();
        if(preAllocatedFuture.compareAndSet(null,future)){
            virtualThreadFactory.execute(()->{
                try{
                    DefaultMappedFile file = synCreateMappedFile(nextFileFromOffset);
                    future.complete(file);
                }catch(Throwable e){
                    future.completeExceptionally(e);
                    //失败恢复为null
                    preAllocatedFuture.compareAndSet(future,null);
                }
            });
        }
    }

    /**
     *  同步创建新的文件并放入到容器中
     */
    private DefaultMappedFile synCreateMappedFile(long fileFromOffset){
        DefaultMappedFile newFile=null;
        try {
            newFile=new DefaultMappedFile(WAL_FILE_PATH,String.valueOf(fileFromOffset),fileFromOffset,config.walFileSize,
                    config.blockSize,config.isWarmWalFile,config.isLockMappedFilePageCache,this);
            mappedFiles.put(fileFromOffset,newFile);
        }catch (Exception e){
            log.warn("Exception is {} , instanceName={},bucketName={},failed to create MappedFile",
                    e,instanceName,bucketName);
        }
        return newFile;
    }

    private void flushReadPositionTask() {
        try {
            while (active) {
                // 利用 floorEntry 解决绝对位点路由问题，时间复杂度为logn，但是数据量少可以忽略
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
                        globalReadPosition = mappedFile.getFileFromOffset() + mappedFile.getFileSize();
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

    //todo
    @Override
    public void close() throws Exception {

    }
}
