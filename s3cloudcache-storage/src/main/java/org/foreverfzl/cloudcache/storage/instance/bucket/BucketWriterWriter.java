package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudcache.core.cache.AppendDataResult;
import org.foreverfzl.cloudcache.core.datastruct.HeapBlockDataStruct;
import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.storage.instance.WriteResult;
import org.foreverfzl.cloudcache.storage.instance.cloudcache.S3CloudCacheInstance;
import org.foreverfzl.cloudcache.wal.datastruct.WalDataStruct;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.AppendMessageResult;
import org.foreverfzl.cloudchache.common.LogName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * 直接获取对应Bucket的操作句柄，适合高性能写数据
 */
public class BucketWriterWriter extends AbstractBucketWriter {

    private static final Logger log = LoggerFactory.getLogger(LogName.BUCKET_INSTANCE);

    private final String bucketName;

    private final MappedFileManager mappedFileManager;

    private final CacheBlockManager cacheBlockManager;

    private final S3CloudCacheInstance instance;

    public BucketWriterWriter(String bucketName, MappedFileManager mappedFileManager, CacheBlockManager cacheBlockManager,S3CloudCacheInstance instance) {
        this.bucketName = bucketName;
        this.mappedFileManager = mappedFileManager;
        this.cacheBlockManager = cacheBlockManager;
        this.instance=instance;
    }


    @Override
    public WriteResult write(byte[] data) {
        WriteResult writeResult = null;
        try {
            AppendMessageResult result = mappedFileManager.appendData(new WalDataStruct(data));
            if (!result.isOk()) {
                log.warn("WAL数据添加失败，result==>{}", result);
            }
            HeapBlockDataStruct dataStruct = new HeapBlockDataStruct(result.getFileFromOffset(), result.getLogicalIndex()
                    , data, 0, data.length);
            AppendDataResult blockResult = cacheBlockManager.appendData(dataStruct);
            if (!blockResult.result()) {
                log.warn("Block数据添加失败，blockResult==>{}", blockResult);
            }
            writeResult = new WriteResult(blockResult.s3Key(), blockResult.offset(), blockResult.size());
        } catch (Exception e) {
            log.error("Exception is=>", e);
        }
        return writeResult;
    }

    @Override
    public WriteResult write(byte[] data, long offset, long length) {
        return null;
    }

    @Override
    public WriteResult write(ByteBuffer buffer) {
        return null;
    }

    @Override
    public WriteResult write(ByteBuffer buffer, long offset, long length) {

        return null;
    }

    //todo关闭资源
    public void close() {
        mappedFileManager.close();
        cacheBlockManager.close();
        instance.close();
    }
}
