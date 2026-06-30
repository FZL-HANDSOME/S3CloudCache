package org.foreverfzl.cloudcache.storage.instance.cloudcache;


import org.foreverfzl.cloudcache.core.global.CoreInstanceBucketManager;
import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.storage.instance.WriteResult;
import org.foreverfzl.cloudcache.storage.instance.bucket.BucketWriterInstance;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.datastruct.WalDataStruct;
import org.foreverfzl.cloudcache.wal.global.WalInstanceBucketManager;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.AppendMessageResult;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.foreverfzl.cloudchache.common.exception.CloudCacheException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;


public class S3CloudCacheInstance extends AbstractCloudCacheInstance {
    private static final Logger log= LoggerFactory.getLogger(LogName.CLOUD_CACHE_INSTANCE);
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


    public S3CloudCacheInstance(S3Client s3Client, S3CloudCacheConfig config) {
        this.s3Client = s3Client;
        this.config = config;
        this.instanceName = config.instanceName;
        String instanceDirPath = config.walPath != null ?
                config.walPath + ProjectUtil.WAL_FILE_ADDRESS :
                ProjectUtil.USER_HOME + ProjectUtil.WAL_FILE_ADDRESS;
        walInstanceBucketManager = new WalInstanceBucketManager(instanceName, instanceDirPath, config);
        coreInstanceBucketManager = new CoreInstanceBucketManager(instanceName, s3Client, config);
    }


    /**
     * 获取BucketName对应的Bucket操作句柄
     */
    public BucketWriterInstance getBucketWriterInstance(String bucketName) {
        if (bucketName == null || bucketName.isBlank()) {
            throw new CloudCacheException("bucketName can not null");
        }
        //去容器中看看有没有对应的manager，有则返回，没有则创建
        MappedFileManager bucketWalManager = walInstanceBucketManager.getOrCreateBucketFileManager(bucketName);
        //todo 为了测试WAL方便，这里先为null
//        CacheBlockManager bucketCoreManager = coreInstanceBucketManager.getOrCreateBlockManager(bucketName);
        return new BucketWriterInstance(bucketName,bucketWalManager,null);
    }


    @Override
    public WriteResult write(String bucketName, ByteBuffer buffer, long offset, long length) {
        return null;
    }

    @Override
    public WriteResult write(String bucketName, ByteBuffer buffer) {
        return null;
    }

    @Override
    public WriteResult write(String bucketName, byte[] data, long offset, long length) {

        return null;
    }

    @Override
    public WriteResult write(String bucketName, byte[] data) {
        AppendMessageResult result=null;
        try {
            DataStruct dataStruct=new WalDataStruct(data);
            result=walInstanceBucketManager.appendData(bucketName,dataStruct);
        }catch (Exception e){
            log.error("Exeception is=>{} , bucketName=>{} failed write",e,bucketName);
        }
        return null;
    }
}
