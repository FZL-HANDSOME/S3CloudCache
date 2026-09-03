package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudcache.core.cache.CloudCacheBlock;
import org.foreverfzl.cloudcache.core.datastruct.BlockDataStruct;
import org.foreverfzl.cloudcache.core.datastruct.DirectBlockDataStruct;
import org.foreverfzl.cloudcache.core.datastruct.HeapBlockDataStruct;
import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.metadata.entity.DeadDataInfo;
import org.foreverfzl.cloudcache.metadata.entity.RecoverTask;
import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.storage.instance.cloudcache.S3CloudCacheInstance;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.datastruct.DirectWalDataStruct;
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
    //将data全部上传到S3
    @Override
    public CompletableFuture<WriteResult> writeHeapData(byte[] data) {
        CompletableFuture<WriteResult> future = new CompletableFuture<>();
        FutureContext futureContext = new FutureContext(future);
        try {
            if (data == null) {
                throw new IllegalArgumentException("data cannot be null");
            }
            doWrite(new WalDataStruct(data), futureContext, future,
                    (mappedFile, fileFromOffset, logicalIndex) ->
                            new HeapBlockDataStruct(mappedFile, fileFromOffset, logicalIndex, data, 0, data.length));
        } catch (Exception e) {
            log.error("BucketWriterWriter writeHeapData Exception is=>", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    //将data的[offset,offset+length]部分上传到S3
    @Override
    public CompletableFuture<WriteResult> writeHeapData(byte[] data, long offset, long length) {
        CompletableFuture<WriteResult> future = new CompletableFuture<>();
        FutureContext futureContext = new FutureContext(future);
        try {
            checkHeapRange(data, offset, length);
            int fromOffset = (int) offset;
            int dataLen = (int) length;
            doWrite(new WalDataStruct(data, fromOffset, dataLen), futureContext, future,
                    (mappedFile, fileFromOffset, logicalIndex) ->
                            new HeapBlockDataStruct(mappedFile, fileFromOffset, logicalIndex, data, fromOffset, dataLen));
        } catch (Exception e) {
            log.error("BucketWriterWriter writeHeapData Exception is=>", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    //将堆外数据buffer的[position,limit)部分全部上传到S3（不推进buffer的position）
    @Override
    public CompletableFuture<WriteResult> writeOffHeapData(ByteBuffer buffer) {
        CompletableFuture<WriteResult> future = new CompletableFuture<>();
        FutureContext futureContext = new FutureContext(future);
        try {
            if (buffer == null) {
                throw new IllegalArgumentException("buffer cannot be null");
            }
            int dataLen = buffer.remaining();
            if (dataLen <= 0) {
                throw new IllegalArgumentException("buffer has no remaining bytes to write");
            }
            // MemorySegment.ofBuffer(buffer) 返回覆盖 [position, limit) 的段，size == remaining()
            MemorySegment segment = MemorySegment.ofBuffer(buffer);
            doWrite(new DirectWalDataStruct(segment), futureContext, future,
                    (mappedFile, fileFromOffset, logicalIndex) ->
                            new DirectBlockDataStruct(mappedFile, fileFromOffset, logicalIndex, 0, dataLen, segment));
        } catch (Exception e) {
            log.error("BucketWriterWriter writeOffHeapData Exception is=>", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    //将堆外数据buffer的[position+offset,position+offset+length)部分上传到S3（offset相对position，不推进position）
    @Override
    public CompletableFuture<WriteResult> writeOffHeapData(ByteBuffer buffer, long offset, long length) {
        CompletableFuture<WriteResult> future = new CompletableFuture<>();
        FutureContext futureContext = new FutureContext(future);
        try {
            checkOffHeapRange(buffer, offset, length);
            int fromOffset = (int) offset;
            int dataLen = (int) length;
            // MemorySegment.ofBuffer(buffer) 返回覆盖 [position, limit) 的段，size == remaining()
            // 因此 fromOffset/dataLen 即相对 position 的偏移与长度
            MemorySegment segment = MemorySegment.ofBuffer(buffer);
            doWrite(new DirectWalDataStruct(segment, dataLen, fromOffset), futureContext, future,
                    (mappedFile, fileFromOffset, logicalIndex) ->
                            new DirectBlockDataStruct(mappedFile, fileFromOffset, logicalIndex, fromOffset, dataLen, segment));
        } catch (Exception e) {
            log.error("BucketWriterWriter writeOffHeapData Exception is=>", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    /**
     * 四个 write 方法的公共骨架：WAL 持久化 -> 处理失败 -> 写入物理 Block。
     * 任何运行时异常向上抛出，由调用方统一 catch 并 completeExceptionally。
     *
     * @param walDataStruct  WAL 持久化协议对象（堆内 WalDataStruct 或堆外 DirectWalDataStruct）
     * @param futureContext  本次写请求的上下文
     * @param future         返回给调用方的 Future
     * @param builder        根据 WAL 追加结果构建对应 BlockDataStruct（堆内/堆外）
     */
    private void doWrite(DataStruct walDataStruct, FutureContext futureContext, CompletableFuture<WriteResult> future,
                         BlockDataStructBuilder builder) throws Exception {
        AppendMessageResult result = mappedFileManager.appendData(walDataStruct);
        long fileFromOffset = result.getFileFromOffset();
        int logicalIndex = result.getLogicalIndex();
        //如果出现除了END_OF_FILE、FILE_CLOSED其它错误，先将该Block的所有相关信息作废(元数据、物理Block等)
        if (!result.isOk()) {
            log.warn("WAL数据添加失败，result==>{}", result);
            // 只有确认了逻辑 Block 序号（logicalIndex >= 0）才需要作废对应的物理 Block；
            // FILE_CLOSED / INVALID_ARGUMENT / MESSAGE_TOO_LARGE 等场景下 logicalIndex 为 -1，无需处理
            if (logicalIndex >= 0) {
                //获取对应的cacheBlock，如果是第一次获取，那么该block中的DefaultMappedFile为null
                //但是无伤大雅，因为既然出错了DefaultMappedFile也用不到
                CloudCacheBlock cacheBlock = cacheBlockManager.getBlock(fileFromOffset, logicalIndex);
                //将CloudCacheBlock标记为unActive并且标记为延迟删除
                cacheBlock.getReference();
                cacheBlock.setUnActive();
                cacheBlock.setDelayClean();
                cacheBlock.releaseReference();
            }
            future.complete(new WriteResult(null, -1, -1, false));
            //对应的元数据对象这里可以不及时删除，因为删除文件的时候会进行删除
            return;
        }
        //wal完成后设置唯一标识
        futureContext.setWalRecordId(result.getBlockOffset());
        BlockDataStruct blockDataStruct = builder.build(result.getDefaultMappedFile(), fileFromOffset, logicalIndex);
        cacheBlockManager.appendData(blockDataStruct, futureContext, true);
    }

    /**
     * 根据 WAL 追加结果构建对应的物理 Block 数据结构（堆内/堆外）。
     */
    @FunctionalInterface
    private interface BlockDataStructBuilder {
        BlockDataStruct build(DefaultMappedFile mappedFile, long fileFromOffset, int logicalIndex);
    }

    private static void checkHeapRange(byte[] data, long offset, long length) {
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        if (offset < 0 || length <= 0 || offset > data.length || length > data.length - offset) {
            throw new IllegalArgumentException("offset/length out of range: offset=" + offset
                    + ", length=" + length + ", data.length=" + data.length);
        }
    }

    private static void checkOffHeapRange(ByteBuffer buffer, long offset, long length) {
        if (buffer == null) {
            throw new IllegalArgumentException("buffer cannot be null");
        }
        int remaining = buffer.remaining();
        if (offset < 0 || length <= 0 || offset > remaining || length > remaining - offset) {
            throw new IllegalArgumentException("offset/length out of range: offset=" + offset
                    + ", length=" + length + ", buffer.remaining=" + remaining);
        }
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
