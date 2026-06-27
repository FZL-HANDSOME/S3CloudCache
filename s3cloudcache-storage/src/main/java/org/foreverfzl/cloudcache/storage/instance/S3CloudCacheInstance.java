package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.core.global.CoreInstanceBucketManager;
import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.wal.global.WalInstanceBucketManager;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.foreverfzl.cloudchache.common.exception.CloudCacheException;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.ByteBuffer;

public class S3CloudCacheInstance implements Instance {
    /**
     * 名字一定要唯一并且不要更改
     */
    protected final String instanceName;

    /**
     * 全局配置文件
     */
    private final S3CloudCacheConfig config;
    /**
     * 持久化WAL文件管理者
     */
    private final WalInstanceBucketManager walInstanceBucketManager;
    /**
     * Cache的管理者
     */
    private final CoreInstanceBucketManager coreInstanceBucketManager;

    private final S3Client s3Client;

    public S3CloudCacheInstance(String instanceName, S3Client s3Client, S3CloudCacheConfig config) {
        this.s3Client = s3Client;
        this.config = config;
        this.instanceName = instanceName;
        String instanceDirPath = config.walPath != null ?
                config.walPath + ProjectUtil.WAL_FILE_ADDRESS :
                ProjectUtil.USER_HOME + ProjectUtil.WAL_FILE_ADDRESS;
        walInstanceBucketManager = new WalInstanceBucketManager(instanceName, instanceDirPath, config);
        coreInstanceBucketManager = new CoreInstanceBucketManager(instanceName, s3Client, config);
    }


    /**
     * 获取BucketName对应的Bucket操作句柄
     */
    public BucketWriterInstance getBucketWriterInstance(BucketInfo bucketInfo) {
        if (bucketInfo == null) {
            throw new CloudCacheException("BucketInfo is null");
        }
        String bucketName = bucketInfo.bucketName;
        String prefix = bucketInfo.prefix;
        if (bucketName == null || bucketName.isBlank()) {
            throw new CloudCacheException("bucketName is null");
        }
        if (prefix == null || prefix.isBlank()) {
            throw new CloudCacheException("prefix is null");
        }
        //todo 补全
        return null;
    }


    @Override
    public WriteResult write(String bucketName, byte[] data, int offset, int length) {
        return null;
    }

    @Override
    public WriteResult write(String bucketName, byte[] data) {
        return null;
    }

    @Override
    public WriteResult write(String bucketName, ByteBuffer buffer) {
        return null;
    }
}
