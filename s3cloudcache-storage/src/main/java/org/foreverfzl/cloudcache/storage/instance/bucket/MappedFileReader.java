package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudcache.metadata.entity.DeadDataInfo;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.datastruct.PaddingStruct;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.NoSuchElementException;

/**
 * 用于包装文件，提供API给用户读取文件数据等
 * 该类就是用于包装死信队列中的数据，提供用户自愈方案
 */
public class MappedFileReader {
    private final String instanceName;
    private final String bucketName;
    private final long fileFromOffset;
    private final int logicalIndex;
    private final String s3Key;
    private final DefaultMappedFile defaultMappedFile;
    private final MemorySegment memorySegment;
    private final long endPosition;
    private long curPosition;

    public MappedFileReader(final DefaultMappedFile defaultMappedFile, final MemorySegment memorySegment, int blockSize, DeadDataInfo deadDataInfo) {
        this.instanceName = deadDataInfo.getInstanceName();
        this.bucketName = deadDataInfo.getBucketName();
        this.fileFromOffset = deadDataInfo.getFileFromOffset();
        this.logicalIndex = deadDataInfo.getLogicalIndex();
        this.s3Key = deadDataInfo.getS3Key();
        this.defaultMappedFile = defaultMappedFile;
        this.endPosition = blockSize;
        this.curPosition = 0;
        this.memorySegment = memorySegment;
    }

    /**
     * 判断当前位置是否已经到达当前块的结尾。
     *
     * <p>块剩余空间小于一个 int，或当前位置为 Padding 标记时，均表示该块已结束。</p>
     */
    public boolean hasNext() {
        if (endPosition - curPosition < Integer.BYTES) {
            return false;
        }
        return memorySegment.get(ValueLayout.JAVA_INT, curPosition) != PaddingStruct.PADDING_MAGIC;
    }

    /**
     * 读取下一条记录的 Value Bytes，并将读取位置推进到下一条记录。
     *
     * @return 当前记录的 Value Bytes
     * @throws NoSuchElementException 当前位置已到达块结尾
     * @throws IllegalStateException  记录头或 Value 长度超出当前块范围
     */
    public byte[] next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more records in mapped block");
        }
        int valueLength = currentValueLength();
        byte[] valueBytes = new byte[valueLength];
        MemorySegment.copy(memorySegment, ValueLayout.JAVA_BYTE,
                curPosition + DataStruct.HEADER_LENGTH, valueBytes, 0, valueLength);
        curPosition += DataStruct.HEADER_LENGTH + valueLength;
        return valueBytes;
    }

    /**
     * 将当前位置之后的所有 Value Bytes 连续复制到一段堆外内存中。
     *
     * <p>内部按当前映射块的大小分配堆外内存；返回的切片长度恰好等于所有
     * Value Bytes 的总长度，不包含协议头和块末尾的 Padding。调用后读取位置
     * 会推进到当前块的结尾。</p>
     */
    public MemorySegment readAll() {
        MemorySegment target = Arena.ofAuto().allocate(defaultMappedFile.getBlockSize());
        long targetPosition = 0;

        while (hasNext()) {
            int valueLength = currentValueLength();
            MemorySegment.copy(memorySegment, ValueLayout.JAVA_BYTE,
                    curPosition + DataStruct.HEADER_LENGTH,
                    target, ValueLayout.JAVA_BYTE, targetPosition, valueLength);
            curPosition += DataStruct.HEADER_LENGTH + valueLength;
            targetPosition += valueLength;
        }
        return target;
    }

    private int currentValueLength() {
        long remaining = endPosition - curPosition;
        if (remaining < DataStruct.HEADER_LENGTH) {
            throw new IllegalStateException("Incomplete record header at position " + curPosition);
        }

        int valueLength = memorySegment.get(ValueLayout.JAVA_INT,
                curPosition + Integer.BYTES * 2L);
        if (valueLength < 0 || valueLength > remaining - DataStruct.HEADER_LENGTH) {
            throw new IllegalStateException("Invalid value length " + valueLength
                    + " at position " + curPosition);
        }
        return valueLength;
    }

    //当用户使用mappedFileReader成功上传了一个block，应该使用该API进行确认，否则会导致上传指针卡住，从而造成文件删除不了
    public void ackUpLoadPosition() {
        defaultMappedFile.ackUpLoadPosition(logicalIndex);
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getBucketName() {
        return bucketName;
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public int getLogicalIndex() {
        return logicalIndex;
    }

    public String getS3Key() {
        return s3Key;
    }
}
