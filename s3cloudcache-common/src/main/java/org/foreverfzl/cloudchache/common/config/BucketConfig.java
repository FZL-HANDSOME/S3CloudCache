package org.foreverfzl.cloudchache.common.config;

import org.foreverfzl.cloudchache.common.cloudcahceEnum.BlockSizeLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.BlockUploadConcurrencyLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.CacheSizeLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.WalFileSize;

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
    public Integer blockSize = BlockSizeLevel.SMALL.getBytes();

    /**
     * Block并发上传大小，默认为8个
     */
    public Integer blockUpLoadCount = BlockUploadConcurrencyLevel.NORMAL.getConcurrency();


    /**
     * 是否预热WAL文件
     */
    public Boolean isWarmWalFile = true;

    /**
     * 是否锁定持久化文件对应的操作系统PageCache缓冲区
     */
    public Boolean isLockMappedFilePageCache = false;

    /**
     * 缓存上传后是否进行一次headObject校验，如果为true会增加一次网络请求。
     */
    public Boolean enableHeadCheck = false;

    /**
     * 将文件的元数据写入到文件开头，数据恢复是方便，默认为5s，时间长了刷新慢，如果宕机恢复数据可能变多，如果时间太短了性能会下降
     */
    public Integer flushFileMetaInfoTime = 5000;

    /**
     * 检查文件，并删除可以删除的文件，默认10s
     */
    public Integer chackMappedFileTime =10000;


    public BucketConfig() {

    }

    public BucketConfig(String s3KeyPrefix, Long walFileSize, Long cacheSize, Integer blockSize, Integer blockUpLoadCount,
                        Boolean isWarmWalFile, Boolean isLockMappedFilePageCache) {
        this.s3KeyPrefix = s3KeyPrefix;
        this.walFileSize = walFileSize;
        this.cacheSize = cacheSize;
        this.blockSize = blockSize;
        this.blockUpLoadCount = blockUpLoadCount;
        this.isWarmWalFile = isWarmWalFile;
        this.isLockMappedFilePageCache = isLockMappedFilePageCache;
    }

    public BucketConfig setS3KeyPrefix(String s3KeyPrefix) {
        this.s3KeyPrefix = s3KeyPrefix;
        return this;
    }

    public BucketConfig setWalFileSize(Long walFileSize) {
        this.walFileSize = walFileSize;
        return this;
    }

    public BucketConfig setCacheSize(Long cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    public BucketConfig setBlockSize(Integer blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public BucketConfig setBlockUpLoadCount(Integer blockUpLoadCount) {
        this.blockUpLoadCount = blockUpLoadCount;
        return this;
    }

    public BucketConfig setWarmWalFile(Boolean warmWalFile) {
        this.isWarmWalFile = warmWalFile;
        return this;
    }

    public BucketConfig setLockMappedFilePageCache(Boolean lockMappedFilePageCache) {
        this.isLockMappedFilePageCache = lockMappedFilePageCache;
        return this;
    }

    public BucketConfig setEnableHeadCheck(Boolean enableHeadCheck) {
        this.enableHeadCheck = enableHeadCheck;
        return this;
    }

    public BucketConfig setFlushFileMetaInfoTime(Integer flushFileMetaInfoTime) {
        this.flushFileMetaInfoTime = flushFileMetaInfoTime;
        return this;
    }

}
