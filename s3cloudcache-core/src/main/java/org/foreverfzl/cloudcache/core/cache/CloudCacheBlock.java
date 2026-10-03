package org.foreverfzl.cloudcache.core.cache;

import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * 一个默认8MB的缓冲块
 */
/**
 * 中文：可复用的堆外物理块；容量由构造参数决定，并非固定 8MB。逻辑绑定只在持有对应 metadata 监视锁并确认身份后使用。
 * English: Reusable native-memory block; capacity is configured rather than fixed at 8 MB. Use its logical binding only after validating identity under the corresponding metadata monitor.
 */
public class CloudCacheBlock extends CacheBlockReferenceResource implements CacheBlock {
    //获取一个干净的block的时候根据用户传进来的字符串 生成一个唯一的s3Key
    /**
     * 中文：当前逻辑绑定的对象键；空闲时为 null，复用时重新赋值。
     * English: Object key of the current logical binding; null while free and reassigned on reuse.
     */
    private String s3Key;

    // 仅仅记录该 Block 在全局 连续堆外内存中的“物理起跑线”
    /**
     * 中文：物理块在全局内存中的固定字节偏移，与 WAL 逻辑偏移无关。
     * English: Fixed byte offset in the global memory allocation, independent of WAL logical offsets.
     */
    private final long blockFromOffset;
    /**
     * 中文：物理切片容量，单位为字节。
     * English: Physical slice capacity in bytes.
     */
    private final int blockSize;

    /**
     * 中文：对所有实例的写入预留游标执行 CAS；不代表数据已复制完成。
     * English: Performs CAS on reservation cursors; advancing a cursor does not establish copy completion.
     */
    protected static final AtomicLongFieldUpdater<CloudCacheBlock> WROTE_POSITION_UPDATER;
    /**
     * 中文：已预留的 value 字节总数；完成字节数另由 metadata 统计。
     * English: Total reserved value bytes; metadata separately tracks completed copies.
     */
    private volatile long writePosition; //写指针

    //globalMemorySegment的一个切片
    /**
     * 中文：借用 manager 所有 Arena 中的切片；本块不单独关闭该内存。
     * English: Borrowed slice of the manager-owned arena; this block does not close it independently.
     */
    protected final MemorySegment memorySegment;
    /**
     * 中文：所属物理池及上传、恢复调度入口，不随逻辑绑定改变。
     * English: Owning physical pool and upload/recovery coordinator, unchanged across logical bindings.
     */
    private final CacheBlockManager manager;

    //逻辑位点层，这一部分在分配WAL文件写指针后确认
    /**
     * 中文：当前绑定的 WAL 对象；字段赋值本身不增加 WAL 引用。
     * English: Currently bound WAL object; assigning this field does not acquire a WAL reference.
     */
    private DefaultMappedFile defaultMappedFile;
    /**
     * 中文：当前 WAL 文件的逻辑起始字节偏移，参与 metadata 身份键。
     * English: Logical starting byte offset of the current WAL file, part of the metadata identity.
     */
    private long fileFromOffset;
    /**
     * 中文：当前 WAL 内逻辑块的零基序号，不是物理池下标。
     * English: Zero-based logical block index within the current WAL, not a physical-pool index.
     */
    private int logicalIndex;  // 它在这个 WAL 文件内部的逻辑序号（0, 1, 2...）
    //对应的元数据对象
    /**
     * 中文：当前绑定的共享状态和监视锁；空闲时为 null，volatile 可见性不能替代身份复核与引用租约。
     * English: Shared state and monitor of the current binding; null while free. Volatile visibility does not replace identity validation and a reference lease.
     */
    private volatile BlockMetaData blockMetaData;


    static {
        WROTE_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(CloudCacheBlock.class, "writePosition");
    }

    /**
     * 中文：包装 manager 提供的物理切片；不分配内存，也不绑定 WAL。
     * English: Wraps a physical slice supplied by the manager without allocating memory or binding a WAL.
     * @param blockFromOffset 全局内存内的字节偏移；English: byte offset in global memory
     * @param blockSize 切片容量字节数；English: slice capacity in bytes
     * @param memorySegment 借用的物理切片；English: borrowed physical slice
     * @param manager 所属物理池；English: owning physical pool
     */
    public CloudCacheBlock(long blockFromOffset, int blockSize, MemorySegment memorySegment, CacheBlockManager manager) {
        this.blockFromOffset = blockFromOffset;
        this.blockSize = blockSize;
        this.memorySegment = memorySegment;
        this.manager = manager;
    }

    /**
     * 原子抢占当前Block写入空间
     *
     * @param size 本次需要写入的数据大小
     */
    /**
     * 中文：原子预留正数 value 字节并返回旧游标；此方法不检查块容量，实际切片访问另行校验边界。
     * English: Atomically reserves a positive number of value bytes and returns the old cursor; capacity is checked later by slice access, not here.
     * @param size 本次预留的 value 字节数，必须大于零；English: positive value-byte reservation
     * @return 本次数据在物理块中的起始字节偏移；English: starting byte offset within the physical block
     * @throws IllegalArgumentException size 不大于零；English: size is not positive
     */
    public long tryAcquireWritePosition(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("size must be greater than 0");
        }
        long currentPosition;
        long nextPosition;
        do {
            currentPosition = WROTE_POSITION_UPDATER.get(this);
            nextPosition = currentPosition + size;
        } while (!WROTE_POSITION_UPDATER.compareAndSet(this, currentPosition, nextPosition));
        return currentPosition;
    }

    //获取写指定区切片
    /**
     * 中文：返回共享写入切片，不复制数据；调用方必须持有当前绑定的引用并保证 Arena 存活。
     * English: Returns a shared writable slice without copying; the caller must hold the current binding's reference and keep its arena alive.
     * @param fromOffset 块内起始字节偏移；English: starting byte offset within the block
     * @param dataLen 切片字节数；English: slice length in bytes
     * @return 原内存的共享视图；English: shared view of the original memory
     */
    public MemorySegment getWriteMemorySegment(long fromOffset, long dataLen) {
        return memorySegment.asSlice(fromOffset, dataLen);
    }

    //获取上传指定分片区域
    /**
     * 中文：返回从零到当前预留游标的视图；上传方须先排除写入并固定绑定，该方法不保证数据已写齐。
     * English: Returns a view from zero to the current reservation cursor; upload callers must first exclude writers and pin the binding, since this method does not establish copy completion.
     * @return 当前预留区域的共享视图；English: shared view of the reserved region
     */
    public MemorySegment getUpdateMemorySegment() {
        return memorySegment.asSlice(0, writePosition);
    }


    /**
     * 业务线程完成写入后的收尾逻辑，修改Block的各个信息
     */
    /**
     * 中文：归还一次写入、上传或恢复租约；最后一个引用在 metadata 锁下决定延迟回收、恢复入队或上传。失败块没有被标为延迟清理时不会直接回池。
     * English: Releases one writer, upload, or recovery lease; the last reference decides deferred recycling, recovery enqueueing, or upload under the metadata monitor. Broken blocks are not recycled unless deferred cleanup is requested.
     * @throws IllegalStateException 未绑定或引用计数已为负，说明租约使用不配对；English: an unbound block or a negative count indicates mismatched lease use
     */
    public void releaseReference() {
        BlockMetaData metadata = this.blockMetaData;
        if (metadata == null) {
            throw new IllegalStateException("Cannot release an unbound block");
        }
        synchronized (metadata) {
            long refs = this.refCount.decrementAndGet();
            if (refs < 0) throw new IllegalStateException("Negative block reference count");
            if (refs != 0) return;
            BlockMetaDataManager blockMetaDataManager = manager.blockMetaDataManager;
            // 中文：引用归零后才兑现延迟清理，仍由池管理器复核旧映射身份。
            // English: Deferred cleanup runs only at zero references and the pool manager still revalidates the former mapping.
            if (isDelayClean()) {
                manager.cleanAndRecycleWithLock(this);
                return;
            }
            // 中文：损坏块继续占用原物理绑定，等待所有原请求到达后取得恢复租约。
            // English: A broken block retains its physical binding until all original requests arrive and recovery can lease it.
            if (metadata.isBroken() && !metadata.isRecovering() && !metadata.isBrokenSubmit()) {
                // bit0=1，bit1=0
                blockMetaDataManager.setTaskToRecoverQueue(metadata, fileFromOffset, logicalIndex);
            }
            //如果可以上传则上传
            if (metadata.canUpload()) {
                manager.updateBlock(this);
            }
        }
    }


    /**
     * 中文：清除逻辑绑定和计数游标，不擦除底层字节也不关闭 Arena；调用者须已在原 metadata 锁下验证池映射且引用为零。
     * English: Clears logical binding and cursor without erasing bytes or closing the arena; callers must validate the pool mapping and zero references under the former metadata monitor.
     * @throws IllegalStateException 仍有引用持有该块；English: references still hold this block
     */
    public void clean() {
        if (refCount.get() != 0) throw new IllegalStateException("Cannot clean a referenced block");
        setUnDelayClean();
        setActive();
        this.s3Key = null;
        this.writePosition = 0;
        this.defaultMappedFile = null;
        this.fileFromOffset = 0;
        this.logicalIndex = 0;
        this.blockMetaData = null;
    }

    /**
     * 中文：仅重置预留游标供完整恢复重放使用；调用方需独占恢复租约并另行清零完成字节计数。
     * English: Resets only the reservation cursor for full replay; callers need an exclusive recovery lease and must separately reset completed-byte accounting.
     */
    public void resetWritePosition() {
        writePosition = 0;
    }

    /**
     * 中文：增加一次引用，不检查 active 或绑定；调用者必须先在正确 metadata 锁下验证身份和准入条件。
     * English: Adds one reference without checking activity or binding; callers must first validate identity and admission under the correct metadata monitor.
     */
    @Override
    public void getReference() {
        this.refCount.incrementAndGet();
    }

    /**
     * 中文：读取当前引用数快照，不能以此单独推断稍后仍可回收。
     * English: Reads a reference-count snapshot, which alone cannot authorize subsequent recycling.
     * @return 当前引用数；English: current reference count
     */
    public int getReferenceCount() {
        return refCount.get();
    }

    /**
     * 中文：读取当前绑定的对象键；需要稳定身份时必须持有租约或 metadata 锁。
     * English: Reads the current object key; a lease or metadata monitor is required when stable identity matters.
     * @return 对象键，空闲时为 null；English: object key, or null while free
     */
    public String getS3Key() {
        return s3Key;
    }

    /**
     * 中文：读取固定的物理起始偏移。
     * English: Reads the fixed physical starting offset.
     * @return 全局内存中的字节偏移；English: byte offset in global memory
     */
    public long getBlockFromOffset() {
        return blockFromOffset;
    }

    /**
     * 中文：读取物理块容量。
     * English: Reads physical block capacity.
     * @return 容量字节数；English: capacity in bytes
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * 中文：读取预留游标快照，并非已提交到 S3 的长度。
     * English: Reads a reservation-cursor snapshot, not the length committed to S3.
     * @return 已预留的 value 字节数；English: reserved value bytes
     */
    public long getWritePosition() {
        return writePosition;
    }

    /**
     * 中文：更新绑定的对象键，由管理器在绑定协议内调用。
     * English: Updates the object key as part of the manager's binding protocol.
     * @param s3Key 当前逻辑块对象键；English: current logical block's object key
     */
    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }

    /**
     * 中文：直接覆盖游标，不校验范围或协调并发写入，调用方需排除其他写入者。
     * English: Directly replaces the cursor without range checks or writer coordination; callers must exclude competing writers.
     * @param writePosition 新游标，单位为字节；English: new cursor in bytes
     */
    public void setWritePosition(int writePosition) {
        this.writePosition = writePosition;
    }

    /**
     * 中文：读取当前绑定的逻辑块序号。
     * English: Reads the currently bound logical block index.
     * @return WAL 内零基块序号；English: zero-based block index within the WAL
     */
    public int getLogicalIndex() {
        return logicalIndex;
    }

    /**
     * 中文：设置逻辑绑定的块序号，不改变池内物理位置。
     * English: Sets the logical block index without changing physical pool position.
     * @param logicalIndex WAL 内零基块序号；English: zero-based WAL block index
     */
    public void setLogicalIndex(int logicalIndex) {
        this.logicalIndex = logicalIndex;
    }

    /**
     * 中文：读取当前绑定 WAL 的逻辑起始偏移。
     * English: Reads the logical starting offset of the currently bound WAL.
     * @return 文件逻辑起始字节偏移；English: logical file start in bytes
     */
    public long getFileFromOffset() {
        return fileFromOffset;
    }

    /**
     * 中文：设置当前逻辑绑定的文件偏移，调用方负责与 metadata 身份一致。
     * English: Sets the file offset of the logical binding; callers must keep it consistent with metadata identity.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     */
    public void setFileFromOffset(long fileFromOffset) {
        this.fileFromOffset = fileFromOffset;
    }

    /**
     * 中文：读取绑定的 WAL 对象，不自动持有其资源引用。
     * English: Reads the bound WAL object without automatically acquiring its resource reference.
     * @return WAL 对象，未绑定时为 null；English: WAL object, or null when unbound
     */
    public DefaultMappedFile getDefaultMappedFile() {
        return defaultMappedFile;
    }

    /**
     * 中文：保存借用的 WAL 对象，跨异步任务使用时仍需单独获取并释放 WAL 引用。
     * English: Stores a borrowed WAL object; asynchronous use still requires separately acquiring and releasing its WAL reference.
     * @param defaultMappedFile 当前 WAL 对象；English: current WAL object
     */
    public void setDefaultMappedFile(DefaultMappedFile defaultMappedFile) {
        this.defaultMappedFile = defaultMappedFile;
    }

    /**
     * 中文：读取固定的所属管理器。
     * English: Reads the fixed owning manager.
     * @return 管理此物理块的 manager；English: manager owning this physical block
     */
    public CacheBlockManager getManager() {
        return manager;
    }

    /**
     * 中文：设置或清除逻辑身份；此赋值不是完整的绑定事务，调用方负责相关锁与映射更新。
     * English: Sets or clears logical identity; this assignment is not a complete binding transaction, so callers coordinate locks and map updates.
     * @param blockMetaData 当前共享 metadata，清理时为 null；English: current shared metadata, or null during cleanup
     */
    public void setBlockMetaData(BlockMetaData blockMetaData) {
        this.blockMetaData = blockMetaData;
    }

    /**
     * 中文：读取可见的绑定快照；随后必须在该对象锁下再次校验，不能仅凭非 null 就操作可复用块。
     * English: Reads a visible binding snapshot; revalidate under that object's monitor before operating on a reusable block.
     * @return 当前 metadata 或 null；English: current metadata or null
     */
    public BlockMetaData getBlockMetaData() {
        return blockMetaData;
    }

    /**
     * 中文：将当前绑定的 metadata 标为损坏；必须已有有效绑定，此方法本身不回收、不提交恢复任务。
     * English: Marks the current metadata broken; a valid binding is required, and this method neither recycles nor submits recovery.
     */
    public void setBroken() {
        blockMetaData.setBroken();
    }

    /**
     * 中文：读取当前绑定的损坏位；空闲块没有 metadata，调用方须先稳定绑定。
     * English: Reads the broken bit of the current binding; free blocks have no metadata, so callers must stabilize binding first.
     * @return 损坏位是否置位；English: whether the broken bit is set
     */
    public boolean isBroken() {
        int isBroken = blockMetaData.getIsBroken();
        return (isBroken & 1) == 1;
    }


}
