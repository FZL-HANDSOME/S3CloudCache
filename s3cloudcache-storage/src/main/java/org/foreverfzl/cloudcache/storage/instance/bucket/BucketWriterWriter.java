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
 * 中文：连接 public 写入 API、WAL 与堆外 Block 的单 Bucket 协调器；每个实例/Bucket 应只创建一个。
 * English: Per-bucket coordinator joining public writes, WAL and native blocks; create only one per instance/bucket.
 * 中文：调用线程完成源数据复制后返回 Future，整块上传确认才成功；并发调用允许，但源 buffer 的生命周期由调用方保证。
 * English: The calling thread copies source bytes before returning a future; success requires block upload acknowledgment. Concurrent calls require stable caller-owned buffers.
 * 中文：本类只拥有恢复线程；Manager 和 S3 客户端由所属实例按顺序关闭，不能把 writer.close 当成全部刷盘完成。
 * English: Owns only the recovery worker; the instance closes managers and S3 in order, so writer.close alone does not guarantee a complete flush.
 */
public class BucketWriterWriter extends AbstractBucketWriter {

    /** 中文：Bucket 协调层日志，异常完成仍需由调用方检查 Future；English: coordinator logger; callers must still inspect future failures. */
    private static final Logger log = LoggerFactory.getLogger(LogName.BUCKET_INSTANCE);

    /** 中文：所属 Bucket 名，构造前由实例校验路径安全；English: bucket name, path-validated by the owning instance before construction. */
    private final String bucketName;

    /** 中文：共享 WAL 管理器，不由本类关闭；English: shared WAL manager, not closed by this writer. */
    private final MappedFileManager mappedFileManager;

    /** 中文：volatile 保证可见性；接纳请求与转 CLOSING 必须同时受 admissionLock 保护。
     * English: Volatile provides visibility; admission and transition to CLOSING must also share admissionLock. */
    private volatile WriterState state;
    /** 中文：只保护接纳状态/计数，不在锁内执行 WAL、复制或网络操作；English: guards admission/count only, never WAL, copies or network I/O. */
    private final Object admissionLock = new Object();
    /** 中文：已接纳但尚未走完 WAL→Core 的请求数，不是上传任务数；English: admitted requests still traversing WAL-to-Core, not uploads. */
    private int inFlightWrites;

    /** 中文：物理池、绑定与恢复租约的共享拥有者；English: shared owner of physical pools, bindings and recovery leases. */
    private final CacheBlockManager cacheBlockManager;

    /** 中文：保留所属实例引用；当前写入链不通过该字段发网络请求；English: retains the parent instance; current writes do not issue network requests through this field. */
    private final S3CloudCacheInstance instance;


    /** 中文：必须与 Core/WAL 使用同一个元数据管理器，否则计数和 Future 无法正确交接；English: must be the same metadata manager shared by Core and WAL. */
    private final BlockMetaDataManager blockMetaDataManager;
    /** 中文：恢复消费循环的退出信号，volatile 不代替线程 join；English: visible recovery-loop exit flag, not a substitute for joining the worker. */
    private volatile boolean active = true;
    /** 中文：构造时启动的专用恢复线程，由 close 中断并等待退出；English: dedicated recovery worker started during construction and interrupted/joined by close. */
    private Thread getBlockBrokenTaskThread;

    /**
     * 中文：绑定已准备好的组件并立即启动恢复消费者；本构造器不独立验证依赖一致性，调用方必须使用同一 Bucket 的组件。
     * English: Binds prepared components and immediately starts recovery consumption; the caller must supply components for the same bucket.
     * @param bucketName 中文：已校验的 Bucket 名；English: validated bucket name.
     * @param mappedFileManager 中文：WAL 拥有者，非 null；English: non-null WAL owner.
     * @param cacheBlockManager 中文：物理池拥有者，非 null；English: non-null native block owner.
     * @param blockMetaDataManager 中文：WAL/Core 共享元数据；English: metadata shared by WAL and Core.
     * @param instance 中文：所属实例，不转移其资源所有权；English: parent instance without transferring its ownership.
     */
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

    /** 中文：仅由构造器调用一次，Thread 不允许重复 start；English: called once by the constructor; a Thread cannot be started twice. */
    private void init() {
        getBlockBrokenTaskThread.start();
    }


    // Future 以 Block 为提交单位，仅在 S3 上传成功并记录确认后完成成功。
    //将data全部上传到S3
    /**
     * 中文：复制完整数组到 WAL 再进入 Core；返回前可能等待空闲 Block，但无需等待远端上传。
     * English: Copies the entire array through WAL and Core; may wait for a free block before returning, but not for remote upload.
     * @param data 中文：非 null、非空数组，调用期间不可修改；English: non-null, nonempty array kept unchanged during the call.
     * @return 中文：成功位置、success=false 或异常完成；参数/准入异常在 Future 中报告；English: location, success=false, or exceptional completion; validation/admission errors are reported through the future.
     */
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
    /**
     * 中文：更正上述区间为左闭右开 [offset, offset+length)；复用源数组，不要求调用方先裁剪。
     * English: The range above is half-open [offset, offset+length); callers need not allocate a trimmed array.
     * @param data 中文：非 null 源数组；English: non-null source array.
     * @param offset 中文：从数组起点计算的非负字节偏移；English: nonnegative byte offset from array start.
     * @param length 中文：数组内的正字节数；English: positive byte count contained in the array.
     * @return 中文：提交结果 Future，非法范围异常完成；English: commit future, completed exceptionally for invalid ranges.
     */
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
    /**
     * 中文：读取 remaining 范围，不推进源游标；视图与源共享内存，调用结束前不能释放或修改源。
     * English: Reads remaining bytes without advancing the source; views share storage that must stay alive and unchanged until return.
     * @param buffer 中文：非 null 且 remaining 大于零；不强制 isDirect；English: non-null buffer with positive remaining; isDirect is not enforced.
     * @return 中文：整块上传确认 Future；参数错误异常完成；English: block acknowledgment future; invalid arguments complete it exceptionally.
     */
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
            // English: Normalize the remaining view to offset zero; slicing shares storage and does not copy or change the original cursor.
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
     * English: Appends a subrange relative to current position through shared views; no ownership transfer or source-cursor movement occurs.
     *
     * @param buffer 堆外数据；English: caller-owned source buffer, without requiring isDirect.
     * @param offset 相对于当前 position 的偏移量（必须 >= 0）；English: nonnegative relative byte offset.
     * @param length 要上传的数据长度；English: positive byte count within the remaining range.
     *               注意：不会推进 buffer 的 position
     *               示例：若 buffer.position()=10, offset=5, length=20，则上传 [15, 35)
     *               English: buffer is non-null; offset is nonnegative relative to position; length is positive and contained in remaining; example selects [15, 35).
     * @return 中文：提交结果 Future，非法参数异常完成；English: commit-result future; invalid arguments complete exceptionally.
     */
    @Override
    public CompletableFuture<WriteResult> writeOffHeapData(ByteBuffer buffer, long offset, long length) {
        CompletableFuture<WriteResult> future = new CompletableFuture<>();
        FutureContext futureContext = new FutureContext(future);
        try {
            checkOffHeapRange(buffer, offset, length);
            // 创建副本并调整到目标范围
            // 中文：duplicate 只隔离游标，不复制底层字节；范围校验后转 int 才安全。
            // English: duplicate isolates cursors, not bytes; conversion to int is safe only after range validation.
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
     * 中文：更正：此处 WAL 成功仅表示写入映射区，不保证已 force；准入计数覆盖 WAL 到 Core 的整个空窗。
     * English: Clarification: WAL success here means mapped-byte append, not force; admission counting covers the entire WAL-to-Core gap.
     * 任何运行时异常向上抛出，由调用方统一 catch 并 completeExceptionally。
     *
     * @param walDataStruct WAL 持久化协议对象（堆内 WalDataStruct 或堆外 DirectWalDataStruct）；English: WAL encoder borrowing the source representation.
     * @param futureContext 本次写请求的上下文；English: context identifying this request.
     * @param future        返回给调用方的 Future；English: future reporting the request outcome.
     * @param builder       根据 WAL 追加结果构建对应 BlockDataStruct（堆内/堆外）；English: factory attaching accepted WAL identity to Core input.
     * English: walDataStruct describes the borrowed input, futureContext identifies this request, future reports its outcome, and builder attaches WAL identity to Core input.
     * @throws Exception 中文：准入关闭或底层处理异常，由外层 write 转为异常 Future；English: admission or downstream failure, converted by public writes to exceptional completion.
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
                // 中文：普通失败也必须退出准入计数，关闭等待的是调用退出，不是 Future 回调执行完。
                // English: Failures must also drain admission; shutdown waits for write calls, not completion of user callbacks.
                inFlightWrites--;
                admissionLock.notifyAll();
            }
        }
    }

    /**
     * 中文：仅处理已登记的请求；WAL 定位成功后才创建 Core 输入，WAL 失败不得为了清理而阻塞分配物理块。
     * English: Handles an admitted request; Core input is created after WAL identity is known, and WAL failure must not allocate a block merely to clean it.
     * @param walDataStruct 中文：借用源数据的 WAL 编码器；English: WAL encoder borrowing the source.
     * @param futureContext 中文：以 WAL 块内记录偏移关联重放的上下文；English: context linked to replay by record offset within a WAL block.
     * @param future 中文：业务失败可正常完成为 success=false；English: business failure may complete normally with success=false.
     * @param builder 中文：不负责资源释放的 Core 输入工厂；English: Core input factory that owns no resource cleanup.
     * @throws Exception 中文：WAL/输入构造中的异常，交由公开方法报告；English: WAL/input construction failures reported by the public method.
     */
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
                // 中文：更正：这里只查询已有绑定，可能为 null；绝不创建缺少元数据的占位 Block。
                // English: Clarification: only an existing binding is queried and may be null; no placeholder block is allocated.
                CloudCacheBlock cacheBlock = cacheBlockManager.getExistingBlock(fileFromOffset, logicalIndex);
                //将CloudCacheBlock标记为unActive并且标记为延迟删除
                BlockMetaData meta = blockMetaDataManager.getBlockMetaData(fileFromOffset, logicalIndex);
                if (meta != null) {
                    synchronized (meta) {
                        meta.markUploadFailed();
                        if (cacheBlock != null && cacheBlock.getBlockMetaData() == meta
                                && cacheBlockManager.getExistingBlock(fileFromOffset, logicalIndex) == cacheBlock) {
                            // 中文：逻辑 Key 与对象身份同时验证，防止物理块回池重绑后被旧失败任务误清理。
                            // English: Verify both logical key and object identity so an old failure cannot clean a rebound pooled block.
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
        // 中文：该 ID 是 WAL 块内记录起点，不是最终 S3 offset；未提交重放可改变物理排列。
        // English: This ID is the WAL-local record start, not final S3 offset; replay may reorder uncommitted physical bytes.
        futureContext.setWalRecordId(result.getBlockOffset());
        BlockDataStruct blockDataStruct = builder.build(result.getDefaultMappedFile(), logicalIndex);
        cacheBlockManager.appendData(blockDataStruct, futureContext, true);
    }

    /**
     * 根据 WAL 追加结果构建对应的物理 Block 数据结构（堆内/堆外）。
     * English: Strategy factory preserving the source representation while adding the assigned WAL identity.
     */
    @FunctionalInterface
    private interface BlockDataStructBuilder {
        /**
         * 中文：只包装输入，不复制或接管源内存；English: wraps input without copying or taking source ownership.
         * @param mappedFile 中文：记录所属 WAL，非 null；English: non-null WAL containing the record.
         * @param logicalIndex 中文：WAL 内逻辑块下标；English: logical block index within the WAL.
         * @return 中文：供本次同步 Core 追加借用的描述符；English: descriptor borrowed by this synchronous Core append.
         */
        BlockDataStruct build(DefaultMappedFile mappedFile, int logicalIndex);
    }

    /**
     * 中文：以减法判断边界，避免 offset+length 溢出；通过后偏移和长度可安全转为 int。
     * English: Uses subtraction to avoid offset+length overflow; validated array offsets/counts can safely narrow to int.
     * @param data 中文：源数组；English: source array.
     * @param offset 中文：绝对字节偏移；English: absolute byte offset.
     * @param length 中文：正字节数；English: positive byte count.
     * @throws IllegalArgumentException 中文：null、空范围或越界；English: null, empty range or out-of-bounds range.
     */
    private static void checkHeapRange(byte[] data, long offset, long length) {
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        if (offset < 0 || length <= 0 || offset > data.length || length > data.length - offset) {
            throw new IllegalArgumentException("offset/length out of range: offset=" + offset
                    + ", length=" + length + ", data.length=" + data.length);
        }
    }

    /**
     * 中文：针对 remaining 而非 capacity 校验，不改变源游标；English: checks against remaining, not capacity, without changing cursors.
     * @param buffer 中文：源 buffer；English: source buffer.
     * @param offset 中文：相对 position 的字节偏移；English: byte offset relative to position.
     * @param length 中文：要读取的正字节数；English: positive byte count to read.
     * @throws IllegalArgumentException 中文：null、非正长度或范围越界；English: null, nonpositive length or invalid range.
     */
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
    /**
     * 中文：阻塞消费一个失败通知并复制整个 Block；应在实例关闭前获取/确认，Reader 的复制内存由调用方关闭。
     * English: Blocks for a failure notification and snapshots one block; obtain/acknowledge before instance shutdown and close the owned reader.
     * @return 中文：只读人工恢复快照，不自动上传或确认；English: read-only manual-recovery snapshot, without automatic upload/ack.
     * @throws InterruptedException 中文：等待队列被中断；English: interrupted waiting for the queue.
     * @throws IllegalStateException 中文：源映射已关闭；English: source mapping has already closed.
     * @throws NullPointerException 中文：通知对应的 WAL 已不在 Manager 中；English: the notification's WAL is no longer registered.
     */
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
        // English: Acquire the source view and snapshot under the same file monitor used by cleanup.
        synchronized (mappedFile) {
            if (mappedFile.isCleanup()) throw new IllegalStateException("Recovery WAL is already closed");
            return new MappedFileReader(mappedFile, mappedFile.getBlockMappedMemorySegmentSlice(blockIndex),
                    mappedFile.getBlockSize(), deadDataInfo);
        }
    }


    /**
     * 中文：串行消费运行时 broken 任务，不负责启动时的历史 WAL 扫描；租约覆盖整块重放，异常通过 finally 结算。
     * English: Serial consumer of runtime broken-block tasks, not startup WAL scanning; a lease spans full replay and is settled in finally.
     * 中文：等待原请求退出不算重试失败；损坏记录或缺少原 Future 则终止该块恢复并保留 WAL。
     * English: Waiting for original requests is not a failed retry; corrupt records or missing original futures fail the block while retaining WAL.
     */
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
                    // English: Requeue until original writers release ownership; 20 ms is a polling pause, not a recovery deadline.
                    blockMetaDataManager.reSetTaskToRecoverQueue(task);
                    Thread.sleep(20);
                    continue;
                }
                file = mappedFileManager.getMappedFile(task.getFileFromOffset());
                if (file == null) throw new IllegalStateException("Recovery WAL is missing");
                file.hold();
                // 中文：readBlock 先全块校验再返回，不能把损坏块的合法前缀提交成完整对象。
                // English: readBlock validates the whole block before returning; never commit only a valid prefix of a corrupt block.
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

    /** 中文：幂等关闭新请求准入，不停止已接纳写入或恢复；English: idempotently closes admission without stopping accepted writes or recovery. */
    public void beginClosing() {
        synchronized (admissionLock) {
            if (state == WriterState.RUNNING) state = WriterState.CLOSING;
        }
    }

    /**
     * 中文：在停止准入后等待整个 WAL→Core 调用排空，wait 会释放准入锁供 finally 结算。
     * English: Waits after admission closure for WAL-to-Core calls to drain; wait releases the monitor so finally can settle counts.
     * @param deadline 中文：System.currentTimeMillis 同基准的绝对毫秒截止时间，不是时长；English: absolute wall-clock millisecond deadline, not a duration.
     * @throws InterruptedException 中文：等待被中断；English: wait was interrupted.
     * @throws IllegalStateException 中文：截止时仍有请求；English: requests remain at the deadline.
     */
    public void awaitWrites(long deadline) throws InterruptedException {
        synchronized (admissionLock) {
            while (inFlightWrites != 0) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) throw new IllegalStateException("Timed out waiting for bucket writes: " + bucketName);
                admissionLock.wait(remaining);
            }
        }
    }

    /**
     * 中文：封口后轮询逻辑恢复状态；必须让恢复消费者继续运行，单纯队列为空不能表示全部恢复完成。
     * English: Polls logical recovery state after sealing; keep the recovery consumer running, since an empty queue is not completion.
     * @param deadline 中文：绝对毫秒截止时间；English: absolute wall-clock deadline in milliseconds.
     * @throws InterruptedException 中文：10 ms 轮询等待被中断；English: interrupted during the 10 ms polling pause.
     * @throws IllegalStateException 中文：恢复等待超时；English: recovery wait timed out.
     */
    public void awaitRecovery(long deadline) throws InterruptedException {
        while (blockMetaDataManager.hasPendingRecovery()) {
            if (System.currentTimeMillis() >= deadline) {
                throw new IllegalStateException("Timed out waiting for block recovery: " + bucketName);
            }
            Thread.sleep(10);
        }
    }

    /**
     * 中文：关闭准入并中断/join 恢复线程；调用前实例应先完成写入和恢复等待，本方法不关闭 WAL/Core/S3。
     * English: Closes admission and interrupts/joins recovery; the instance must first drain writes/recovery; WAL/Core/S3 are not closed here.
     * 中文：join 没有独立超时，收到调用线程中断后仍等退出再恢复中断标志；勿从该恢复线程自身调用。
     * English: join has no independent timeout; caller interruption is restored after exit. Never invoke from the recovery worker itself.
     */
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

    /** @return 中文：借用共享 WAL 管理器；生命周期仍归实例；English: borrowed shared WAL manager, still owned by the instance. */
    public MappedFileManager getMappedManager() {
        return mappedFileManager;
    }


    /** @return 中文：所属 Bucket 名；English: this writer's bucket name. */
    public String getBucketName() {
        return bucketName;
    }
}
