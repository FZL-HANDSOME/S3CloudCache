package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.storage.instance.WriteResult;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * 直接获取对应Bucket的操作句柄，适合高性能写数据
 */
public class BucketWriterInstance extends AbstractBucketInstance {

    private final MappedFileManager mappedFileManager;

    private final CacheBlockManager cacheBlockManager;

    public BucketWriterInstance(MappedFileManager mappedFileManager, CacheBlockManager cacheBlockManager) {
        this.mappedFileManager = mappedFileManager;
        this.cacheBlockManager = cacheBlockManager;
    }


    @Override
    public WriteResult write(byte[] data) {
        return null;
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
}
