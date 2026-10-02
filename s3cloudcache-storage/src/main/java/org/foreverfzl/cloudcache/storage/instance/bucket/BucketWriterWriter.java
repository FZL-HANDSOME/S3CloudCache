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

/**
 * 直接获取对应Bucket的操作句柄，适合高性能写数据
 */
public class BucketWriterWriter extends AbstractBucketWriter {

    private static final Logger log = LoggerFactory.getLogger(LogName.BUCKET_INSTANCE);

    private final String bucketName;

    private final MappedFileManager mappedFileManager;

    private volatile WriterState state;
    private final Object admissionLock = new Object();
    private int inFlightWrites;

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


    // Future 以 Block 为提交单位，仅在 S3 上传成功并记录确认后完成成功。
    //将data全部上传到S3
    @Override
    public CompletableFuture<WriteResult> writeHeapData(byte[] data) {
        CompletableFuture<WriteResult> future = new CompletableFuture<>();
        FutureContext futureContext = new FutureContext(future);
        try {
            if (data == null || data.length == 0) {
                throw new IllegalArgumentException("data cannot be null or empty");
            }
            doWrite(new WalDataStruct(data), futureContext, future,
                    (mappedFile, logicalIndex) ->
                            new HeapBlockDataStruct(mappedFile, logicalIndex, data, 0, data.length));
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
                    (mappedFile, logicalIndex) ->
                            new HeapBlockDataStruct(mappedFile, logicalIndex, data, fromOffset, dataLen));
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
            // 用 slice() 把 [position, limit) 归一化成 [0, dataLen)，再包装成 MemorySegment，
            // 规避 MemorySegment.ofBuffer 对 position/limit 的语义差异，保证段大小 == dataLen
            MemorySegment segment = MemorySegment.ofBuffer(buffer.slice());
            doWrite(new DirectWalDataStruct(segment, 0, dataLen), futureContext, future,
                    (mappedFile, logicalIndex) ->
                            new DirectBlockDataStruct(mappedFile, logicalIndex, segment, 0, dataLen));
        } catch (Exception e) {
            log.error("BucketWriterWriter writeOffHeapData Exception is=>", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    /**
     * 将堆外数据 buffer 的 [position+offset, position+offset+length) 部分上传到 S3
     *
     * @param buffer 堆外数据
     * @param offset 相对于当前 position 的偏移量（必须 >= 0）
     * @param length 要上传的数据长度
     *               注意：不会推进 buffer 的 position
     *               示例：若 buffer.position()=10, offset=5, length=20，则上传 [15, 35)
     */
    @Override
    public CompletableFuture<WriteResult> writeOffHeapData(ByteBuffer buffer, long offset, long length) {
        CompletableFuture<WriteResult> future = new CompletableFuture<>();
        FutureContext futureContext = new FutureContext(future);
        try {
            checkOffHeapRange(buffer, offset, length);
            // 创建副本并调整到目标范围
            int len = (int) length;
            ByteBuffer dup = buffer.duplicate();
            int startPos = dup.position() + (int) offset;
            dup.position(startPos);
            dup.limit(startPos + len);
            MemorySegment segment = MemorySegment.ofBuffer(dup);
            doWrite(new DirectWalDataStruct(segment, 0, len), futureContext, future,
                    (mappedFile, logicalIndex) ->
                            new DirectBlockDataStruct(mappedFile, logicalIndex, segment, 0, len));
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
     * @param walDataStruct WAL 持久化协议对象（堆内 WalDataStruct 或堆外 DirectWalDataStruct）
     * @param futureContext 本次写请求的上下文
     * @param future        返回给调用方的 Future
     * @param builder       根据 WAL 追加结果构建对应 BlockDataStruct（堆内/堆外）
     */
    private void doWrite(DataStruct walDataStruct, FutureContext futureContext, CompletableFuture<WriteResult> future,
                          BlockDataStructBuilder builder) throws Exception {
        synchronized (admissionLock) {
            if (state != WriterState.RUNNING) {
                throw new IllegalStateException("Bucket writer is closing: " + bucketName);
            }
            inFlightWrites++;
        }
        try {
            doAcceptedWrite(walDataStruct, futureContext, future, builder);
        } finally {
            synchronized (admissionLock) {
                inFlightWrites--;
                admissionLock.notifyAll();
            }
        }
    }

    private void doAcceptedWrite(DataStruct walDataStruct, FutureContext futureContext,
                                 CompletableFuture<WriteResult> future, BlockDataStructBuilder builder) throws Exception {
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
                CloudCacheBlock cacheBlock = cacheBlockManager.getExistingBlock(fileFromOffset, logicalIndex);
                //将CloudCacheBlock标记为unActive并且标记为延迟删除
                BlockMetaData meta = blockMetaDataManager.getBlockMetaData(fileFromOffset, logicalIndex);
                if (meta != null) {
                    synchronized (meta) {
                        meta.markUploadFailed();
                        if (cacheBlock != null && cacheBlock.getBlockMetaData() == meta
                                && cacheBlockManager.getExistingBlock(fileFromOffset, logicalIndex) == cacheBlock) {
                            cacheBlock.getReference();
                            cacheBlock.setUnActive();
                            cacheBlock.setDelayClean();
                            cacheBlock.releaseReference();
                        }
                    }
                    meta.failAllFuture();
                }
            }
            future.complete(new WriteResult(null, -1, -1, false));
            //对应的元数据对象这里可以不及时删除，因为删除文件的时候会进行删除
            return;
        }
        //wal完成后设置唯一标识
        futureContext.setWalRecordId(result.getBlockOffset());
        BlockDataStruct blockDataStruct = builder.build(result.getDefaultMappedFile(), logicalIndex);
        cacheBlockManager.appendData(blockDataStruct, futureContext, true);
    }

    /**
     * 根据 WAL 追加结果构建对应的物理 Block 数据结构（堆内/堆外）。
     */
    @FunctionalInterface
    private interface BlockDataStructBuilder {
        BlockDataStruct build(DefaultMappedFile mappedFile, int logicalIndex);
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
        // 获取映射视图和复制快照必须一起与 clean 互斥，不能先拿到失效的 MemorySegment。
        synchronized (mappedFile) {
            if (mappedFile.isCleanup()) throw new IllegalStateException("Recovery WAL is already closed");
            return new MappedFileReader(mappedFile, mappedFile.getBlockMappedMemorySegmentSlice(blockIndex),
                    mappedFile.getBlockSize(), deadDataInfo);
        }
    }


    private void getBlockBroken() {
        while (active) {
            CloudCacheBlock block = null;
            DefaultMappedFile file = null;
            boolean recovered = false;
            try {
                RecoverTask task = blockMetaDataManager.getTaskFromRecoverQueue();
                BlockMetaData meta = blockMetaDataManager.getBlockMetaData(task.getFileFromOffset(), task.getBlockIndex());
                if (meta == null || meta.getState() != BlockMetaData.SEALED || !meta.isBroken()) continue;
                block = cacheBlockManager.beginRecovery(task.getFileFromOffset(), task.getBlockIndex());
                if (block == null) {
                    // 原写入尚未走完，不是恢复失败；保留任务，不能按等待次数丢弃。
                    blockMetaDataManager.reSetTaskToRecoverQueue(task);
                    Thread.sleep(20);
                    continue;
                }
                file = mappedFileManager.getMappedFile(task.getFileFromOffset());
                if (file == null) throw new IllegalStateException("Recovery WAL is missing");
                file.hold();
                int recoveredBytes = 0;
                for (WalBlockReader.Record record : WalBlockReader.readBlock(file, task.getBlockIndex())) {
                    FutureContext context = meta.getFuture(record.walOffset());
                    if (context == null) throw new IllegalStateException("Recovery record has no original request");
                    var result = cacheBlockManager.appendData(new HeapBlockDataStruct(file, task.getBlockIndex(),
                            record.value(), 0, record.value().length), context, false);
                    if (!result.result()) throw new IllegalStateException("Physical Block replay failed");
                    recoveredBytes += record.value().length;
                }
                recovered = recoveredBytes == meta.getExpectedBytes();
                if (!recovered) throw new IllegalStateException("Recovery byte count differs from accepted WAL bytes");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Block recovery failed; retain WAL for repair", e);
            } finally {
                if (file != null) file.release();
                if (block != null) cacheBlockManager.finishRecovery(block, recovered);
            }
        }
    }

    public void beginClosing() {
        synchronized (admissionLock) {
            if (state == WriterState.RUNNING) state = WriterState.CLOSING;
        }
    }

    public void awaitWrites(long deadline) throws InterruptedException {
        synchronized (admissionLock) {
            while (inFlightWrites != 0) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) throw new IllegalStateException("Timed out waiting for bucket writes: " + bucketName);
                admissionLock.wait(remaining);
            }
        }
    }

    public void awaitRecovery(long deadline) throws InterruptedException {
        while (blockMetaDataManager.hasPendingRecovery()) {
            if (System.currentTimeMillis() >= deadline) {
                throw new IllegalStateException("Timed out waiting for block recovery: " + bucketName);
            }
            Thread.sleep(10);
        }
    }

    public void close() {
        beginClosing();
        active = false;
        getBlockBrokenTaskThread.interrupt();
        boolean interrupted = false;
        while (getBlockBrokenTaskThread.isAlive()) {
            try {
                getBlockBrokenTaskThread.join();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        state = WriterState.CLOSED;
        if (interrupted) Thread.currentThread().interrupt();
    }

    public MappedFileManager getMappedManager() {
        return mappedFileManager;
    }


    public String getBucketName() {
        return bucketName;
    }
}
