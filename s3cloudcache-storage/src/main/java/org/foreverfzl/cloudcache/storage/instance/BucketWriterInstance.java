package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;

import java.nio.ByteBuffer;

/**
 * 直接获取对应Bucket的操作句柄，适合高性能写
 */
public class BucketWriterInstance implements Instance {

    private final MappedFileManager mappedFileManager;

    private final CacheBlockManager cacheBlockManager;

    public BucketWriterInstance(MappedFileManager mappedFileManager, CacheBlockManager cacheBlockManager) {
        this.mappedFileManager = mappedFileManager;
        this.cacheBlockManager = cacheBlockManager;
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
