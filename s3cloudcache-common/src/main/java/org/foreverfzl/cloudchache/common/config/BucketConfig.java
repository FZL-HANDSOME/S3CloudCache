package org.foreverfzl.cloudchache.common.config;

import org.foreverfzl.cloudchache.common.cloudcahceEnum.*;

public class BucketConfig {
    /**
     * 生成S3key的用户自定义前缀
     */
    public String s3KeyPrefix;

    /**
     * WAL 文件大小，默认 1G
     */
    public Long walFileSize = WalFileSize.SIZE_1G.getBytes();


    /**
     * 缓冲区大小
     */
    public Long cacheSize = CacheSizeLevel.TINE.getBytes();

    /**
     * Block的大小，默认为8MB
     */
    public Integer blockSize = BlockSizeLevel.MEDIUM.getBytes();

    /**
     * Block并发上传大小，默认为8个
     */
    public Integer blockUpLoadCount = BlockUploadConcurrencyLevel.NORMAL.getConcurrency();


    /**
     * Page刷盘级别
     */
    public Long pageFlushLevel = PageFlushLevel.FAST_10_MS.getFlushIntervalMs();

    /**
     * 是否预热WAL文件
     */
    public Boolean isWarmWalFile=true;

    /**
     * 是否锁定持久化文件对应的操作系统PageCache缓冲区
     */
    public Boolean isLockMappedFilePageCache = false;


}
