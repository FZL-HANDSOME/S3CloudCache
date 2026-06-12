package org.foreverfzl.cloudcache.wal.manager;

import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.foreverfzl.cloudchache.common.exception.WalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.lang.foreign.MemorySegment;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 这个类专门管理目前存在的文件，创建、维护等
 * 责任：
 * 1：刷盘确认readPosition
 */
public class MappedFileManager {
    private static final Logger log = LoggerFactory.getLogger("MappedFileManager");
    // 3. 【核心骨架】：并发跳表。Key 是文件的起始 Offset，天生按位点升序排列
    private final ConcurrentSkipListMap<Long, DefaultMappedFile> mappedFiles = new ConcurrentSkipListMap<>();
    //该instance下所有的mappedFile的全局read指针，该指针之前的数据全部安全
    private volatile long globalReadPosition;
    //刷新所有文件的读指针
    private final Thread flushReadPositionThread;
    //检查所有的文件，控制文件是否要删除
    private final Thread chackMappedFileThread;
    //主要控制线程flushReadPosition、chackMappedFile等的执行
    private volatile boolean active=true;


    private S3CloudCacheConfig config;

    public MappedFileManager() {
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

    private void init(){
        flushReadPositionThread.start();
        chackMappedFileThread.start();
    }

    public void flushReadPositionTask() {
        try {
            while (active) {
                // 利用 floorEntry 解决绝对位点路由问题
                Map.Entry<Long, DefaultMappedFile> entry = mappedFiles.floorEntry(globalReadPosition);
                if (entry == null) {
                    // 如果实在没找到，说明系统还没初始化好第一个文件，sleep 等待
                    Thread.sleep(config.getPageFlushLevel());
                    continue;
                }
                DefaultMappedFile mappedFile = entry.getValue();
                if (mappedFile == null) {
                    throw new WalException("flushReadPositionTask failed: can not find `" + globalReadPosition + "` DefaultMappedFile Object");
                }
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
                }
                Thread.sleep(config.getPageFlushLevel());
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public void chackMappedFileTask() {

    }

}
