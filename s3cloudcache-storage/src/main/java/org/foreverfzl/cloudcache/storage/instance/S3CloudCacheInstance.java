package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.ByteBuffer;

public class S3CloudCacheInstance implements Instance{
    /**
     * 名字一定要唯一并且不要更改
     */
    protected final String instanceName;
    /**
     * WAL配置文件
     */
    private final S3CloudCacheConfig.WalConfig walConfig;
    /**
     * Cache配置文件
     */
    private final S3CloudCacheConfig.CacheConfig cacheConfig;
//    /**
//     * 持久化WAL文件管理者
//     */
//    private final WalInstanceBucketManager walInstanceBucketManager;
//    /**
//     * Cache的管理者
//     */
//    private final CoreInstanceBucketManager coreInstanceBucketManager;

    private final S3Client s3Client;

    public S3CloudCacheInstance(String instanceName, S3Client s3Client,
                                S3CloudCacheConfig.WalConfig walConfig, S3CloudCacheConfig.CacheConfig cacheConfig) {
        this.s3Client=s3Client;
        this.walConfig = walConfig;
        this.cacheConfig = cacheConfig;
        this.instanceName = instanceName;

    }


    @Override
    public WriteResult write(String bucketName, String objectPrefix, byte[] data, int offset, int length) {
        return null;
    }

    @Override
    public WriteResult write(String bucketName, String objectPrefix, byte[] data) {
        return null;
    }

    @Override
    public WriteResult write(String bucketName, String objectPrefix, ByteBuffer buffer) {
        return null;
    }
}
