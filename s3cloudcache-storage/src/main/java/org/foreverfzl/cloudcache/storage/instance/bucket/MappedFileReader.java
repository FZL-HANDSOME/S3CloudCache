package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudcache.metadata.entity.DeadDataInfo;
import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * 人工恢复用的只读 Block 快照。必须使用 try-with-resources 释放快照的堆外内存。
 * 快照不借用 WAL 的映射生命周期，因此确认后后台删除 WAL 不会使正在读取的数据失效。
 * ack 仍需在实例关闭前、整块上传成功后显式调用；close 本身绝不确认或删除 WAL。
 * English: Caller-owned read-only native snapshot for manual recovery; use try-with-resources. Snapshot reads survive source WAL cleanup,
 * but acknowledgment requires the live instance and a successful whole-block upload. Closing never acknowledges/deletes WAL.
 * 中文：方法同步保护游标和懒加载缓存；返回视图的外部使用仍须与 close 协调，shared Arena 不代表无限生命周期。
 * English: Synchronized methods protect cursors/lazy caches; external use of returned views must still coordinate with close despite the shared arena.
 */
public class MappedFileReader implements AutoCloseable {
    /** 中文：来源实例名，只读诊断身份；English: immutable originating instance identity. */
    private final String instanceName;
    /** 中文：人工上传的目标 Bucket；English: destination bucket for manual upload. */
    private final String bucketName;
    /** 中文：WAL 文件逻辑字节偏移，不是快照内偏移；English: logical WAL-file byte offset, not an offset inside this snapshot. */
    private final long fileFromOffset;
    /** 中文：WAL 内逻辑块索引，从零开始；English: zero-based logical block index within WAL. */
    private final int logicalIndex;
    /** 中文：失败通知携带的原对象 Key；English: original object key carried by the failure notification. */
    private final String s3Key;
    /** 中文：仅确认时需要原 WAL；读取快照不延长映射所有权；English: original WAL needed for acknowledgment; snapshot reading owns no mapping lease. */
    private final DefaultMappedFile defaultMappedFile;
    /** 中文：Reader 独占关闭责任的 shared Arena；English: shared arena exclusively closed by this reader. */
    private final Arena arena;
    /** 中文：BlockSize 大小的只读原始 WAL 副本，含头和填充；English: read-only BlockSize raw WAL copy including headers/padding. */
    private final MemorySegment memorySegment;
    /** 中文：原始快照末尾字节位置，等于 BlockSize；English: raw snapshot end in bytes, equal to BlockSize. */
    private final long endPosition;
    /** 中文：复制时已接纳的 Value 字节数；-1 表示缺少元数据，不能证明业务完整性；English: accepted Value bytes at snapshot time; -1 means metadata unavailable, not proven complete. */
    private final int expectedBytes;
    /** 中文：首次全块验证后的堆内记录缓存，null 表示尚未验证；English: heap records cached after full validation; null means not yet validated. */
    private List<WalBlockReader.Record> records;
    /** 中文：懒分配的紧凑 Value 只读视图，随 Reader 关闭失效；English: lazily allocated compact Value view, invalid after reader close. */
    private MemorySegment allValues;
    /** 中文：下一条解析记录索引，不是字节偏移；English: next parsed-record index, not a byte offset. */
    private int recordIndex;
    /** 中文：相对原始 WAL Block 的游标，包含协议头和对齐长度；English: cursor relative to raw WAL block, including headers/alignment. */
    private long curPosition;
    /** 中文：关闭标志，受 Reader monitor 保护；English: closed flag guarded by the reader monitor. */
    private boolean closed;

    /**
     * 中文：在文件锁下复制一块原始数据，不做全块解码；该锁只防映射清理，不代表冻结所有在途写入，因此后续仍验证 expectedBytes。
     * English: Copies a raw block under the file monitor without decoding; this prevents unmapping, not all in-flight writes, so later validation checks expectedBytes.
     * @param defaultMappedFile 中文：有效来源 WAL；English: live source WAL.
     * @param memorySegment 中文：从目标块起点开始、至少 blockSize 字节的可读视图；English: readable view starting at the target block, at least blockSize bytes.
     * @param blockSize 中文：必须等于文件配置的 Block 字节数；English: must equal the file's configured block size in bytes.
     * @param deadDataInfo 中文：与来源文件/块匹配的失败身份；English: failure identity matching the source file/block.
     * @throws IllegalArgumentException 中文：尺寸或身份不匹配；English: size/identity mismatch.
     * @throws IllegalStateException 中文：WAL 映射已清理；English: source mapping already cleaned.
     * @throws NullPointerException 中文：必需参数为 null；English: required argument is null.
     */
    public MappedFileReader(DefaultMappedFile defaultMappedFile, MemorySegment memorySegment,
                            int blockSize, DeadDataInfo deadDataInfo) {
        this.defaultMappedFile = Objects.requireNonNull(defaultMappedFile, "defaultMappedFile");
        Objects.requireNonNull(memorySegment, "memorySegment");
        Objects.requireNonNull(deadDataInfo, "deadDataInfo");
        if (blockSize != defaultMappedFile.getBlockSize() || memorySegment.byteSize() < blockSize) {
            throw new IllegalArgumentException("Reader must cover exactly one logical WAL Block");
        }
        this.instanceName = deadDataInfo.getInstanceName();
        this.bucketName = deadDataInfo.getBucketName();
        this.fileFromOffset = deadDataInfo.getFileFromOffset();
        this.logicalIndex = deadDataInfo.getLogicalIndex();
        if (fileFromOffset != defaultMappedFile.fileFromOffset || logicalIndex < 0
                || logicalIndex >= defaultMappedFile.fileSize / blockSize) {
            throw new IllegalArgumentException("Reader identity does not belong to this WAL file");
        }
        this.s3Key = deadDataInfo.getS3Key();
        this.endPosition = blockSize;
        this.arena = Arena.ofShared();
        try {
            // clean/ack 使用同一文件锁；复制期间不能卸载源映射。
            // English: Sharing the clean/ack monitor prevents source unmapping during the copy.
            synchronized (defaultMappedFile) {
                if (defaultMappedFile.isCleanup()) throw new IllegalStateException("WAL file is already closed");
                BlockMetaData metadata = defaultMappedFile.getManager().blockMetaDataManager
                        .getBlockMetaData(fileFromOffset, logicalIndex);
                expectedBytes = metadata == null ? -1 : metadata.getExpectedBytes();
                MemorySegment snapshot = arena.allocate(blockSize);
                snapshot.copyFrom(memorySegment.asSlice(0, blockSize));
                this.memorySegment = snapshot.asReadOnly();
            }
        } catch (RuntimeException | Error failure) {
            arena.close();
            throw failure;
        }
    }

    /** 首次读取先校验整块，不能把错误 Magic、CRC 或长度错误静默当作正常结束。 */
    /**
     * 中文：首次 O(BlockSize) 校验并缓存，后续查询为 O(1)；损坏不会静默返回 false。
     * English: First call validates/caches in O(BlockSize), subsequent queries are O(1); corruption is not silently treated as EOF.
     * @return 中文：游标后是否还有记录；English: whether another record remains at the cursor.
     * @throws IllegalStateException 中文：已关闭、损坏或已知期望字节不匹配；English: closed, corrupt or inconsistent with known expected bytes.
     */
    public synchronized boolean hasNext() {
        return recordIndex < validatedRecords().size();
    }

    /** 仅返回 Value Bytes；结束时抛 NoSuchElementException，不返回含混的 null。 */
    /**
     * 中文：推进一条记录并返回独立数组，数组不受 Reader.close 影响；复制成本与该条 Value 长度成正比。
     * English: Advances one record and returns an independent array surviving reader close; copy cost is proportional to its Value length.
     * @return 中文：不含头/填充的可修改 Value 副本；English: mutable Value copy excluding headers/padding.
     * @throws NoSuchElementException 中文：已到记录末尾；English: no records remain.
     * @throws IllegalStateException 中文：关闭或验证失败；English: closed reader or failed validation.
     */
    public synchronized byte[] next() {
        if (!hasNext()) throw new NoSuchElementException("End of WAL Block");
        WalBlockReader.Record record = records.get(recordIndex++);
        curPosition = record.walOffset() + serializedLength(record);
        // 防止调用者修改返回数组，意外改变后续 readAll 上传的内容。
        // English: Defensive copy prevents caller mutation from changing later readAll content.
        return record.value().clone();
    }

    /**
     * 返回整块（不是游标之后）所有 Value Bytes 的紧凑堆外视图，并把游标移到末尾。
     * 按 BlockSize 分配，返回的 byteSize 仅为有效数据长度，不含头部、对齐或零填充。
     * 返回值只读，生命周期到本 Reader.close；可直接交给 s3RawPutObject。
     * English: Returns every Value in the block, not only remaining records, compacted into a native read-only valid-length slice; consumes the cursor.
     * 中文：首次另分配 BlockSize 缓冲并复制，重复调用复用视图；加上原快照和解码数组不是零拷贝。
     * English: First call allocates another BlockSize buffer/copies bytes; repeated calls reuse it. Raw snapshot and parsed arrays make this non-zero-copy.
     * @return 中文：由本 Reader 拥有、仅有效 Value 长度的堆外视图，可为空；English: reader-owned native view of valid Value length, possibly zero bytes.
     * @throws IllegalStateException 中文：Reader 已关闭或全块验证失败；English: reader closed or full-block validation failed.
     */
    public synchronized MemorySegment readAll() {
        List<WalBlockReader.Record> values = validatedRecords();
        if (allValues == null) {
            MemorySegment buffer = arena.allocate(defaultMappedFile.getBlockSize());
            long position = 0;
            for (WalBlockReader.Record record : values) {
                buffer.asSlice(position, record.value().length).copyFrom(MemorySegment.ofArray(record.value()));
                position += record.value().length;
            }
            allValues = buffer.asSlice(0, position).asReadOnly();
        }
        recordIndex = values.size();
        curPosition = values.isEmpty() ? 0 : values.getLast().walOffset() + serializedLength(values.getLast());
        return allValues;
    }

    /**
     * 调用者负责先确认整块上传成功。这里只校验本地内容，不能替调用者验证远端上传结果。
     * 对损坏/空块拒绝确认，防止人工读取有效前缀后把整份 WAL 标记为可删除。
     * English: Caller must verify remote upload; local validation rejects corrupt/empty/changing blocks before persisting WAL acknowledgment.
     * 中文：可能触发后续 WAL 删除；它不重新完成原失败 Future，也不验证已传给远端的字节内容。
     * English: May enable later WAL deletion; it neither re-completes old failed futures nor validates the actual remote bytes.
     * @throws IllegalStateException 中文：空块、不完整/变动快照、缺元数据或关闭状态；English: empty/incomplete/changing snapshot, missing metadata or closed state.
     * @throws RuntimeException 中文：底层 WAL 已清理或确认 force 失败，不能视为已确认成功；English: source WAL was cleaned or acknowledgment force failed; acknowledgment cannot be assumed successful.
     */
    public synchronized void ackUpLoadPosition() {
        if (validatedRecords().isEmpty()) throw new IllegalStateException("Cannot acknowledge an empty WAL Block");
        BlockMetaData metadata = defaultMappedFile.getManager().blockMetaDataManager
                .getBlockMetaData(fileFromOffset, logicalIndex);
        if (metadata == null || metadata.getState() == BlockMetaData.OPEN || metadata.isRecovering()
                || metadata.getExpectedBytes() != expectedBytes || metadata.getPageCacheBytes() != expectedBytes) {
            throw new IllegalStateException("WAL Block is still changing or differs from the recovery snapshot");
        }
        defaultMappedFile.ackUpLoadPosition(logicalIndex);
    }

    /**
     * 中文：持 Reader monitor 懒校验；成功后才发布缓存，失败允许再次尝试但不会刷新既有快照。
     * English: Lazily validates under the reader monitor, publishing cache only on success; retry revalidates the same snapshot, not new source bytes.
     * @return 中文：内部只供读取的列表，不向用户暴露可变 Value 引用；English: internally read-only list, not exposing mutable Value references to users.
     * @throws IllegalStateException 中文：关闭、协议损坏或字节计数不齐；English: closed, corrupt protocol or incomplete byte count.
     */
    private List<WalBlockReader.Record> validatedRecords() {
        if (closed) throw new IllegalStateException("MappedFileReader is closed");
        if (records == null) {
            List<WalBlockReader.Record> parsed = WalBlockReader.readBlock(memorySegment);
            long actualBytes = parsed.stream().mapToLong(record -> record.value().length).sum();
            // 死信可能来自资源分配失败，此时同块仍有 WAL 写入在途；全零尾部不能冒充完整快照。
            // English: Failure may precede other WAL writers; a zero tail alone is not proof that all accepted bytes were captured.
            if (expectedBytes >= 0 && actualBytes != expectedBytes) {
                throw new IllegalStateException("Recovery snapshot is incomplete; original WAL is retained");
            }
            records = parsed;
        }
        return records;
    }

    /**
     * 中文：恢复原始 WAL 游标步长：12 字节头加 Value 向上对齐到 4 字节；long 运算避免加三溢出。
     * English: Raw WAL stride is a 12-byte header plus Value rounded up to four bytes; long arithmetic avoids overflow on adding three.
     * @param record 中文：已验证记录；English: validated record.
     * @return 中文：含协议头/填充的字节长度；English: byte length including header/padding.
     */
    private static long serializedLength(WalBlockReader.Record record) {
        return DataStruct.HEADER_LENGTH + ((record.value().length + 3L) & ~3L);
    }

    /** @return 中文：来源实例名；English: source instance name. */
    public String getInstanceName() { return instanceName; }
    /** @return 中文：目标 Bucket 名；English: destination bucket name. */
    public String getBucketName() { return bucketName; }
    /** @return 中文：WAL 逻辑文件偏移，字节；English: logical WAL-file byte offset. */
    public long getFileFromOffset() { return fileFromOffset; }
    /** @return 中文：文件内逻辑 Block 索引；English: logical block index in the file. */
    public int getLogicalIndex() { return logicalIndex; }
    /** @return 中文：失败块原 S3 Key；English: original S3 key of the failed block. */
    public String getS3Key() { return s3Key; }
    /** @return 中文：原始 WAL 块内字节游标；English: byte cursor within the raw WAL block. */
    public synchronized long getCurPosition() { return curPosition; }
    /** @return 中文：快照容量末尾，不是 Value 字节数；English: snapshot capacity end, not Value byte count. */
    public long getEndPosition() { return endPosition; }

    /** 原始 WAL 快照，包含协议头与填充；上传纯数据必须用 readAll，而不是这个方法。 */
    /**
     * 中文：供协议诊断的原始只读视图，不能从该方法返回推断记录已经验证。
     * English: Raw read-only diagnostic view; obtaining it does not prove records were validated.
     * @return 中文：Reader 拥有的原始快照，close 后不可访问；English: reader-owned raw snapshot, inaccessible after close.
     * @throws IllegalStateException 中文：Reader 已关闭；English: reader already closed.
     */
    public synchronized MemorySegment getMemorySegment() {
        if (closed) throw new IllegalStateException("MappedFileReader is closed");
        return memorySegment;
    }

    /** 只允许定位到记录边界或有效数据末尾，不允许从 Value 中间误解析协议头。 */
    /**
     * 中文：先校验全块，再线性查找合法边界；定位不会复制/修改记录。
     * English: Validates the block then linearly searches valid boundaries; seeking neither copies nor modifies records.
     * @param position 中文：原始块内字节偏移，记录起点、数据末尾或 BlockSize；English: raw-block byte offset at a record start, data end or BlockSize.
     * @throws IllegalArgumentException 中文：不是允许边界；English: position is not an allowed boundary.
     * @throws IllegalStateException 中文：已关闭或快照验证失败；English: closed or invalid snapshot.
     */
    public synchronized void setCurPosition(long position) {
        List<WalBlockReader.Record> values = validatedRecords();
        for (int index = 0; index < values.size(); index++) {
            if (values.get(index).walOffset() == position) {
                recordIndex = index;
                curPosition = position;
                return;
            }
        }
        long dataEnd = values.isEmpty() ? 0 : values.getLast().walOffset() + serializedLength(values.getLast());
        if (position == dataEnd || position == endPosition) {
            recordIndex = values.size();
            curPosition = position;
            return;
        }
        throw new IllegalArgumentException("Position is not a WAL record boundary: " + position);
    }

    /**
     * 中文：幂等释放自己的堆外快照和 readAll 缓冲，不确认/关闭源 WAL；外部使用视图期间不得调用。
     * English: Idempotently releases owned native snapshots/buffers, without acknowledging/closing source WAL; never close while external code uses a view.
     */
    @Override
    public synchronized void close() {
        if (!closed) {
            arena.close();
            closed = true;
            records = null;
            allValues = null;
        }
    }

    /** @return 中文：身份/游标诊断，不读取 Value 内容；English: identity/cursor diagnostics without reading Value contents. */
    @Override
    public synchronized String toString() {
        return "MappedFileReader{bucket=" + bucketName + ", fileOffset=" + fileFromOffset
                + ", block=" + logicalIndex + ", s3Key=" + s3Key + ", position=" + curPosition
                + ", closed=" + closed + "}";
    }
}
