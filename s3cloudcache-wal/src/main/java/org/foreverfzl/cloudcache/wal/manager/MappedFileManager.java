package org.foreverfzl.cloudcache.wal.manager;

import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudcache.wal.storefile.WalDataStruct;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.foreverfzl.cloudchache.common.exception.WalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.lang.foreign.MemorySegment;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 这个类专门管理该Bucket存在的文件
 */
public class MappedFileManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger("MappedFileManager");
    public final String instanceName;
    public final String bucketName;
    private final String prefix; //用户自定义的前缀，用于生成S3key
    //前缀文件引用
    private final DefaultMappedFile prefixMappedFile;

    // 3. 【核心骨架】：并发跳表。Key 是文件的起始 Offset，天生按位点升序排列
    private final ConcurrentSkipListMap<Long, DefaultMappedFile> mappedFiles = new ConcurrentSkipListMap<>();
    //该instance下所有的mappedFile的全局read指针，该指针之前的数据全部安全
    private volatile long globalReadPosition;
    //刷新所有文件的读指针
    private final Thread flushReadPositionThread;
    //检查所有的文件，控制文件是否要删除
    private final Thread chackMappedFileThread;
    //主要控制线程flushReadPosition、chackMappedFile等的执行
    private volatile boolean active = true;
    //操作系统脏页刷新时间，也就是globalReadPosition的刷新时间
    private final long pageFlushTime;
    //该bucket的wal目录绝对地址
    private final String dirPath;
    private final long fileSize;
    private final int blockSize;

    public MappedFileManager(String prefix,String dirPath,String instanceName, String bucketName, long pageFlushTime,
                             long fileSize,int blockSize) {
        this.prefix = prefix;
        this.instanceName = instanceName;
        this.bucketName = bucketName;
        this.dirPath = dirPath;
        this.pageFlushTime = pageFlushTime;
        this.fileSize=fileSize;
        this.blockSize=blockSize;
        this.prefixMappedFile=walPrefix(prefix);
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

    public void flushReadPositionTask() {
        try {
            while (active) {
                // 利用 floorEntry 解决绝对位点路由问题，时间复杂度为logn，但是数据量少可以忽略
                Map.Entry<Long, DefaultMappedFile> entry = mappedFiles.floorEntry(globalReadPosition);
                if (entry == null) {
                    // 如果实在没找到，说明系统还没初始化好第一个文件，sleep 等待
                    log.warn("flushReadPositionTask failed: can not find `{}` DefaultMappedFile Object", globalReadPosition);
                    Thread.sleep(pageFlushTime);
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

                Thread.sleep(pageFlushTime);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    //将prefix持久化，只有创建新的MappedFileManager的时候(也就是新的Bucket)才会持久化prefix
    private DefaultMappedFile walPrefix(String prefix) {
        DefaultMappedFile file=new DefaultMappedFile(dirPath, ProjectUtil.PREFIX_FILE_NAME,0, 1024 * 1024,blockSize,false,false,this);
        file.appendData(new WalDataStruct(prefix));
        return file;
    }

    public void chackMappedFileTask() {

    }

    //todo
    @Override
    public void close() throws Exception {

    }
}
