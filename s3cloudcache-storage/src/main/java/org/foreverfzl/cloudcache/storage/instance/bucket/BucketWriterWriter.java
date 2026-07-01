package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudcache.core.cache.AppendDataResult;
import org.foreverfzl.cloudcache.core.datastruct.HeapBlockDataStruct;
import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.storage.instance.WriteResult;
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
    private static final Logger log= LoggerFactory.getLogger(LogName.BUCKET_INSTANCE);

    private final String bucketName;

    private final MappedFileManager mappedFileManager;

    private final CacheBlockManager cacheBlockManager;

    public BucketWriterWriter(String bucketName, MappedFileManager mappedFileManager, CacheBlockManager cacheBlockManager) {
        this.bucketName=bucketName;
        this.mappedFileManager = mappedFileManager;
        this.cacheBlockManager = cacheBlockManager;
    }


    @Override
    public WriteResult write(byte[] data) {
//        try {
//            AppendMessageResult result = mappedFileManager.appendData(new WalDataStruct(data));
//            if(!result.isOk()){
//                log.warn("WAL数据添加失败，result==>{}",result);
//            }
////public HeapBlockDataStruct(String fileName, int blockIndex, int blockExpectedValidBytes, byte[] dataBytes, int fromOffset, int dataLen)
//            HeapBlockDataStruct dataStruct = new HeapBlockDataStruct(result.getFileName(),result.getLogicalIndex()
//                    ,result.getBlockExpectedValidBytes(),data,0,data.length);
//            AppendDataResult blockResult = cacheBlockManager.appendData(dataStruct);
//            if(!blockResult.result()){
//                log.warn("Block数据添加失败，blockResult==>{}",blockResult);
//            }
//        } catch (Exception e) {
//            log.error("Exception is=>",e);
//        }
//        return null;
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
