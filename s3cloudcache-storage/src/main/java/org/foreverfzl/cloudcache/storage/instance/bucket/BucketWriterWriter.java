package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudcache.core.cache.AppendDataResult;
import org.foreverfzl.cloudcache.core.datastruct.HeapBlockDataStruct;
import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.metadata.entity.RecoverTask;
import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.storage.instance.WriteResult;
import org.foreverfzl.cloudcache.storage.instance.cloudcache.S3CloudCacheInstance;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.datastruct.WalDataStruct;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.AppendMessageResult;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.LogName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * 直接获取对应Bucket的操作句柄，适合高性能写数据
 */
public class BucketWriterWriter extends AbstractBucketWriter {

    private static final Logger log = LoggerFactory.getLogger(LogName.BUCKET_INSTANCE);

    private final String bucketName;

    private final MappedFileManager mappedFileManager;

    private final CacheBlockManager cacheBlockManager;

    private final S3CloudCacheInstance instance;

    private final BlockMetaDataManager blockMetaDataManager;
    private volatile boolean active = true;
    private Thread getBlockBrokenTaskThread;

    public BucketWriterWriter(String bucketName, MappedFileManager mappedFileManager, CacheBlockManager cacheBlockManager, BlockMetaDataManager blockMetaDataManager,
                              S3CloudCacheInstance instance) {
        this.bucketName = bucketName;
        this.mappedFileManager = mappedFileManager;
        this.cacheBlockManager = cacheBlockManager;
        this.instance = instance;
        this.blockMetaDataManager = blockMetaDataManager;
        getBlockBrokenTaskThread = new Thread(this::getBlockBroken);
        init();
    }

    private void init() {
//        getBlockBrokenTaskThread.start();
    }


    @Override
    public WriteResult write(byte[] data) {
        WriteResult writeResult = null;
        try {
            AppendMessageResult result = mappedFileManager.appendData(new WalDataStruct(data));
            if (!result.isOk()) {
                log.warn("WAL数据添加失败，result==>{}", result);
            }
            HeapBlockDataStruct dataStruct = new HeapBlockDataStruct(result.getDefaultMappedFile(),result.getFileFromOffset(), result.getLogicalIndex()
                    , data, 0, data.length);
            AppendDataResult blockResult = cacheBlockManager.appendData(dataStruct);
            if (!blockResult.result()) {
                log.warn("Block数据添加失败，blockResult==>{}", blockResult);
            }
            writeResult = new WriteResult(blockResult.s3Key(), blockResult.offset(), blockResult.size());
        } catch (Exception e) {
            log.error("BucketWriterWriter write Exception is=>", e);
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

    private void getBlockBroken() {
        while (active) {
            try {
                //获取任务
                RecoverTask task = blockMetaDataManager.getTaskFromRecoverQueue();
                long fileFromOffset = task.getFileFromOffset();
                int blockIndex = task.getBlockIndex();
                //获取对应的文件对应block的MS切片
                DefaultMappedFile mappedFile = mappedFileManager.getFile(fileFromOffset, blockIndex);
                int blockSize = mappedFile.getBlockSize();
                long endPos = fileFromOffset + ((long) (blockIndex + 1) * blockSize);
                long pos = fileFromOffset + ((long) blockIndex * blockSize);
                CRC32 crc = new CRC32();
                //如果可以读int并且是正常数据则读取
                //如果一个Block结束了会在结尾打上end标志，end标志占用4字节，如果结尾位置4字节都不够默认就是结束了
                while (endPos - pos >= 4 && mappedFile.getInt(pos) == DataStruct.MAGIC_NUMBER) {
                    pos += 4;
                    int chackSum = mappedFile.getInt(pos);
                    pos += 4;
                    int dataLen = mappedFile.getInt(pos);
                    pos += 4;
                    byte[] orgData = mappedFile.getOrgData(pos, dataLen);
                    pos += dataLen;
                    crc.update(orgData);
                    int cs = (int) crc.getValue();
                    if (cs != chackSum) {
                        //说明数据不对，后面的数据不用恢复了
                        break;
                    }
                    //调用API正常恢复数据
                    cacheBlockManager.appendData(new HeapBlockDataStruct(mappedFile,fileFromOffset, blockIndex, orgData, 0, dataLen));
                    crc.reset();
                }
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.warn("getBlockBrokenTask failed=>", e);
            }
        }
    }

    //todo关闭资源
    public void close() {
        mappedFileManager.close();
        cacheBlockManager.close();
        instance.close();
    }
}
