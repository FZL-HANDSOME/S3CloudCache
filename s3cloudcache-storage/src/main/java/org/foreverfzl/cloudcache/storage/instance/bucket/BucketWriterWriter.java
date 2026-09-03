package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudcache.core.cache.CloudCacheBlock;
import org.foreverfzl.cloudcache.core.datastruct.HeapBlockDataStruct;
import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.metadata.entity.DeadDataInfo;
import org.foreverfzl.cloudcache.metadata.entity.RecoverTask;
import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.storage.instance.cloudcache.S3CloudCacheInstance;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.datastruct.WalDataStruct;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.AppendMessageResult;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.FutureContext;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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


    //咱们的项目修改为只要数据成功写入到wal文件中WriteResult就为true，
    // 如果一个S3key的WriteResult有一个为false则代表与该S3key相关的数据全部上传失败(让用户自己操作)。
    @Override
    public CompletableFuture<WriteResult> write(byte[] data) {
        CompletableFuture<WriteResult> future = new CompletableFuture<>();
        FutureContext futureContext = new FutureContext(future);
        try {
            AppendMessageResult result = mappedFileManager.appendData(new WalDataStruct(data));
            long fileFromOffset = result.getFileFromOffset();
            int logicalIndex = result.getLogicalIndex();
            //如果出现除了END_OF_FILE、FILE_CLOSED其它错误，先将该Block的所有相关信息作废(元数据、物理Block等)
            if (!result.isOk()) {
                log.warn("WAL数据添加失败，result==>{}", result);
                //获取对应的cacheBlock，如果是第一次获取，那么该block中的DefaultMappedFile为null
                //但是无伤大雅，因为既然出错了DefaultMappedFile也用不到
                CloudCacheBlock cacheBlock = cacheBlockManager.getBlock(fileFromOffset, logicalIndex);
                //将CloudCacheBlock标记为unActive并且标记为延迟删除
                cacheBlock.getReference();
                cacheBlock.setUnActive();
                cacheBlock.setDelayClean();
                cacheBlock.releaseReference();
                future.complete(new WriteResult(null, -1, -1, false));
                //对应的元数据对象这里可以不及时删除，因为删除文件的时候会进行删除
                return future;
            }
            //wal完成后设置唯一标识
            futureContext.setWalRecordId(result.getBlockOffset());
            HeapBlockDataStruct dataStruct = new HeapBlockDataStruct(result.getDefaultMappedFile(), fileFromOffset, logicalIndex
                    , data, 0, data.length);
            cacheBlockManager.appendData(dataStruct, futureContext,true);
        } catch (Exception e) {
            log.error("BucketWriterWriter write Exception is=>", e);
            // 异常时必须完成 Future，否则调用方 whenComplete 永远不触发，
            // 上层只能靠超时发现"卡死"，无法区分"异常"与"未完成"
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<WriteResult> write(byte[] data, long offset, long length) {
        return null;
    }

    @Override
    public CompletableFuture<WriteResult> write(ByteBuffer buffer) {
        return null;
    }

    @Override
    public CompletableFuture<WriteResult> write(ByteBuffer buffer, long offset, long length) {
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
            long fileFromOffset = -1;
            int blockIndex = -1;
            BlockMetaData blockMetaData = null;
            RecoverTask task = null;
            CloudCacheBlock cacheBlock = null;
            try {
                //获取任务
                task = blockMetaDataManager.getTaskFromRecoverQueue();
                if (task.getTimes() == 5) {
                    //如果重新放回超过5次则不进行数据恢复
                    log.warn("fileFromOffset= {} blockIndex= {} can not recover,because put back over 5 times", fileFromOffset, blockIndex);
                    continue;
                }
                fileFromOffset = task.getFileFromOffset();
                blockIndex = task.getBlockIndex();
                //先看看对应的block是否真正全部写入到PageCache或者落盘
                blockMetaData = blockMetaDataManager.getBlockMetaData(fileFromOffset, blockIndex);
                if (blockMetaData == null) {
                    log.warn("fileFromOffset= {} blockIndex= {} can not recover,because blockMetaData is null", fileFromOffset, blockIndex);
                    continue;
                }
                //先检查所有数据是否全部写入到PageCache中
                if (blockMetaData.getExpectedBytes() != blockMetaData.getPageCacheBytes()) {
                    //由于对应的wal数据还没写完，该block不能进行数据恢复，重新放回
                    task.incrementTimes();
                    blockMetaDataManager.reSetTaskToRecoverQueue(task);
                    Thread.sleep(1000);
                    continue;
                }
                //物理block写入失败后该CloudCacheBlock会存放在cacheBlockManager的keyBlockMap
                //然后检查此时该CacheBlock是否有其它线程写入或者上传
                cacheBlock = cacheBlockManager.getExistingBlock(fileFromOffset, blockIndex);
                int referenceCount = cacheBlock.getReferenceCount();
                if (referenceCount != 0) {
                    //说明有其它线程正在写入并且上传，将恢复任务重新放入到队列中
                    task.incrementTimes();
                    blockMetaDataManager.reSetTaskToRecoverQueue(task);
                    Thread.sleep(1000);
                    continue;
                }
            } catch (InterruptedException interruptedException) {
                active = false;
                Thread.currentThread().interrupt();
                break;
            }
            DefaultMappedFile mappedFile = mappedFileManager.getMappedFile(fileFromOffset);
            if (mappedFile == null) {
                continue;
            }
            //到这里该block的状态为：broken为true并且封口，并且block数据全部写入到了PageCache，并且此时没有其它线程引用该block
            //在此我们需要将isBroken设置为flase，active设置为true，并且将该CacheBlock的指针设置为0，并将元数据的Finish大小设置为0
            cacheBlock.resetWritePosition();
            cacheBlock.setActive();
            blockMetaData.clearFinishedBytes();
            mappedFile.hold();
            try {
                int blockSize = mappedFile.getBlockSize();
                long blockOffset = 0;
                long endPos = fileFromOffset + ((long) (blockIndex + 1) * blockSize);
                long curPos = fileFromOffset + ((long) blockIndex * blockSize);
                CRC32 crc = new CRC32();
                //获取该block对应所有的future
                ConcurrentHashMap<Long, FutureContext> futureMap = blockMetaData.getFutureMap();
                if (futureMap == null) {
                    log.error("fileFromOffset={},blockIndex={},futureMap is null", fileFromOffset, blockIndex);
                    continue;
                }
                //如果可以读int并且是正常数据则读取
                //如果一个Block结束了会在结尾打上end标志，end标志占用4字节，如果结尾位置4字节都不够默认就是结束了
                while (true) {
                    FutureContext futureContext = futureMap.get(blockOffset);
                    if (endPos - curPos <= DataStruct.HEADER_LENGTH) {
                        break;
                    }
                    int magic = mappedFile.getIntFromDataArea(curPos);
                    curPos += 4;
                    if (magic != DataStruct.MAGIC_NUMBER) {
                        break;
                    }
                    int chackSum = mappedFile.getIntFromDataArea(curPos);
                    curPos += 4;
                    int dataLen = mappedFile.getIntFromDataArea(curPos);
                    curPos += 4;
                    byte[] orgData = mappedFile.getOrgDataFromDataArea(curPos, dataLen);
                    curPos += (dataLen + 3) & ~3;
                    crc.update(orgData);
                    int cs = (int) crc.getValue();
                    if (cs != chackSum) {
                        //说明数据不对，后面的数据不用恢复了
                        break;
                    }
                    //调用API正常恢复数据
                    cacheBlockManager.appendData(new HeapBlockDataStruct(mappedFile, fileFromOffset, blockIndex, orgData, 0, dataLen), futureContext,false);
                    blockOffset = blockOffset + DataStruct.HEADER_LENGTH + (dataLen + 3) & ~3;
                    crc.reset();
                }
            } catch (Exception e) {
                log.error("getBlockBrokenTask failed=>", e);
            } finally {
                mappedFile.release();
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
