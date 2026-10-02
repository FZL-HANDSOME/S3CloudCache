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
 */
public class MappedFileReader implements AutoCloseable {
    private final String instanceName;
    private final String bucketName;
    private final long fileFromOffset;
    private final int logicalIndex;
    private final String s3Key;
    private final DefaultMappedFile defaultMappedFile;
    private final Arena arena;
    private final MemorySegment memorySegment;
    private final long endPosition;
    private final int expectedBytes;
    private List<WalBlockReader.Record> records;
    private MemorySegment allValues;
    private int recordIndex;
    private long curPosition;
    private boolean closed;

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
    public synchronized boolean hasNext() {
        return recordIndex < validatedRecords().size();
    }

    /** 仅返回 Value Bytes；结束时抛 NoSuchElementException，不返回含混的 null。 */
    public synchronized byte[] next() {
        if (!hasNext()) throw new NoSuchElementException("End of WAL Block");
        WalBlockReader.Record record = records.get(recordIndex++);
        curPosition = record.walOffset() + serializedLength(record);
        // 防止调用者修改返回数组，意外改变后续 readAll 上传的内容。
        return record.value().clone();
    }

    /**
     * 返回整块（不是游标之后）所有 Value Bytes 的紧凑堆外视图，并把游标移到末尾。
     * 按 BlockSize 分配，返回的 byteSize 仅为有效数据长度，不含头部、对齐或零填充。
     * 返回值只读，生命周期到本 Reader.close；可直接交给 s3RawPutObject。
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

    private List<WalBlockReader.Record> validatedRecords() {
        if (closed) throw new IllegalStateException("MappedFileReader is closed");
        if (records == null) {
            List<WalBlockReader.Record> parsed = WalBlockReader.readBlock(memorySegment);
            long actualBytes = parsed.stream().mapToLong(record -> record.value().length).sum();
            // 死信可能来自资源分配失败，此时同块仍有 WAL 写入在途；全零尾部不能冒充完整快照。
            if (expectedBytes >= 0 && actualBytes != expectedBytes) {
                throw new IllegalStateException("Recovery snapshot is incomplete; original WAL is retained");
            }
            records = parsed;
        }
        return records;
    }

    private static long serializedLength(WalBlockReader.Record record) {
        return DataStruct.HEADER_LENGTH + ((record.value().length + 3L) & ~3L);
    }

    public String getInstanceName() { return instanceName; }
    public String getBucketName() { return bucketName; }
    public long getFileFromOffset() { return fileFromOffset; }
    public int getLogicalIndex() { return logicalIndex; }
    public String getS3Key() { return s3Key; }
    public synchronized long getCurPosition() { return curPosition; }
    public long getEndPosition() { return endPosition; }

    /** 原始 WAL 快照，包含协议头与填充；上传纯数据必须用 readAll，而不是这个方法。 */
    public synchronized MemorySegment getMemorySegment() {
        if (closed) throw new IllegalStateException("MappedFileReader is closed");
        return memorySegment;
    }

    /** 只允许定位到记录边界或有效数据末尾，不允许从 Value 中间误解析协议头。 */
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

    @Override
    public synchronized void close() {
        if (!closed) {
            arena.close();
            closed = true;
            records = null;
            allValues = null;
        }
    }

    @Override
    public synchronized String toString() {
        return "MappedFileReader{bucket=" + bucketName + ", fileOffset=" + fileFromOffset
                + ", block=" + logicalIndex + ", s3Key=" + s3Key + ", position=" + curPosition
                + ", closed=" + closed + "}";
    }
}
