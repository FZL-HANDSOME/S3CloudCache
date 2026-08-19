package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudcache.core.cache.AppendDataResult;
import org.foreverfzl.cloudcache.core.datastruct.HeapBlockDataStruct;
import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.metadata.entity.DeadDataInfo;
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

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * 直接获取对应Bucket的操作句柄，适合高性能写数据
 */
public class BucketWriterWriter extends AbstractBucketWriter {

    private static final Logger log = LoggerFactory.getLogger(LogName.BUCKET_INSTANCE);

    private final String bucketName;

    private final MappedFileManager mappedFileManager;

    private WriterState state;

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
        this.state = WriterState.RUNNING;
        this.blockMetaDataManager = blockMetaDataManager;
        getBlockBrokenTaskThread = new Thread(this::getBlockBroken);
        init();
    }

    private void init() {
        getBlockBrokenTaskThread.start();
    }


    @Override
    public WriteResult write(byte[] data) {
        WriteResult writeResult = null;
        try {
            AppendMessageResult result = mappedFileManager.appendData(new WalDataStruct(data));
            long fileFromOffset = result.getFileFromOffset();
            int logicalIndex = result.getLogicalIndex();
            //如果出现其它错误，先将该Block的所有相关信息作废(元数据、物理Block等)
            if (!result.isOk()) {
                log.warn("WAL数据添加失败，result==>{}", result);
                //将对应的物理block标记为删除
                String s3Key = cacheBlockManager.deleteBlock(fileFromOffset, logicalIndex);
                //将对应的元数据删除
                blockMetaDataManager.deleteBlockMetaData(fileFromOffset, logicalIndex);
                return new WriteResult(s3Key, -1, -1, false);
            }
            HeapBlockDataStruct dataStruct = new HeapBlockDataStruct(result.getDefaultMappedFile(), fileFromOffset, logicalIndex
                    , data, 0, data.length);
            AppendDataResult blockResult = cacheBlockManager.appendData(dataStruct);
            if (!blockResult.result()) {
                log.warn("Block数据添加失败，blockResult==>{}", blockResult);
            }
            if (!blockResult.result()) {
                writeResult = new WriteResult(blockResult.s3Key(), -1, -1, false);
            } else {
                writeResult = new WriteResult(blockResult.s3Key(), blockResult.offset(), blockResult.size(), true);
            }
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

    //监听死信队列的数据，内部指明了哪个bucket哪个文件哪个block中的数据上传不上去，
    //然后提供Reader给用户读取、上传、确认API
    public MappedFileReader getUpLoadFailedBlockInfo() throws InterruptedException {
        DeadDataInfo deadDataInfo = blockMetaDataManager.getDeadDataInfo();
        long fileFromOffset = deadDataInfo.getFileFromOffset();
        int blockIndex = deadDataInfo.getLogicalIndex();
        log.info("fileFromOffset={},blockIndex={} is consumed from deadQueue", fileFromOffset, blockIndex);
        DefaultMappedFile mappedFile = mappedFileManager.getMappedFile(fileFromOffset);
        if (mappedFile == null) {
            throw new NullPointerException("MappedFile is null");
        }
        int blockSize = mappedFile.getBlockSize();
        MemorySegment memorySegment = mappedFile.getBlockMappedMemorySegmentSlice(blockIndex);
        MappedFileReader reader = new MappedFileReader(mappedFile, memorySegment, blockSize, deadDataInfo);
        return reader;
    }


    private void getBlockBroken() {
        while (active) {
            try {
                //获取任务
                RecoverTask task = blockMetaDataManager.getTaskFromRecoverQueue();
                long fileFromOffset = task.getFileFromOffset();
                int blockIndex = task.getBlockIndex();
                log.info("fileFromOffset=>{}, blockIndex=>{} is Consumed from recoverQueue", fileFromOffset, blockIndex);
                //先看看对应的block是否真正全部写入到PageCache或者落盘
                BlockMetaData blockMetaData = blockMetaDataManager.getBlockMetaData(fileFromOffset, blockIndex);
                if (blockMetaData == null) {
                    continue;
                }
                if (blockMetaData.getState() != 1 && blockMetaData.getExpectedBytes() != blockMetaData.getPageCacheBytes()) {
                    //重新放回
                    blockMetaDataManager.setTaskToRecoverQueue(task);
                    Thread.sleep(100);
                    continue;
                }
                DefaultMappedFile mappedFile = mappedFileManager.getMappedFile(fileFromOffset);
                if (mappedFile == null) {
                    continue;
                }
                int blockSize = mappedFile.getBlockSize();
                long endPos = fileFromOffset + ((long) (blockIndex + 1) * blockSize);
                long pos = fileFromOffset + ((long) blockIndex * blockSize);
                CRC32 crc = new CRC32();
                //如果可以读int并且是正常数据则读取
                //如果一个Block结束了会在结尾打上end标志，end标志占用4字节，如果结尾位置4字节都不够默认就是结束了
                while (endPos - pos >= 4 && mappedFile.getIntFromDataArea(pos) == DataStruct.MAGIC_NUMBER) {
                    pos += 4;
                    int chackSum = mappedFile.getIntFromDataArea(pos);
                    pos += 4;
                    int dataLen = mappedFile.getIntFromDataArea(pos);
                    pos += 4;
                    byte[] orgData = mappedFile.getOrgDataFromDataArea(pos, dataLen);
                    pos += dataLen;
                    crc.update(orgData);
                    int cs = (int) crc.getValue();
                    if (cs != chackSum) {
                        //说明数据不对，后面的数据不用恢复了
                        break;
                    }
                    //调用API正常恢复数据
                    cacheBlockManager.appendData(new HeapBlockDataStruct(mappedFile, fileFromOffset, blockIndex, orgData, 0, dataLen));
                    crc.reset();
                }
            } catch (InterruptedException interruptedException) {
                active = false;
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.warn("getBlockBrokenTask failed=>", e);
            }
        }
    }

    public void close() {
        this.state = WriterState.CLOSING;
        getBlockBrokenTaskThread.interrupt();
    }

    public MappedFileManager getMappedManager() {
        return mappedFileManager;
    }


    public String getBucketName() {
        return bucketName;
    }
}
