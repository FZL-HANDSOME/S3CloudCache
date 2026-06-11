package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import software.amazon.awssdk.services.s3.S3Client;

public class S3CloudCacheInstance {
    /**
     * 名字一定要唯一并且不要更改
     */
    protected String instanceName;
    /**
     * 配置文件
     */
    private S3CloudCacheConfig config;
    /**
     * 持久化WAL文件管理者
     */
    private MappedFileManager mappedFileManager;

    private S3Client s3Client;

}
