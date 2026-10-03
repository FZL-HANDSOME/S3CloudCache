package org.foreverfzl.cloudcache.core.datastruct;

import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * 针对堆内数据
 */
/**
 * 中文：借用字节数组的指定范围，将 Value 复制到物理块；构造时不复制数据。
 * English: Borrows a byte-array range to copy values into a block; construction does not copy bytes.
 */
public class HeapBlockDataStruct implements BlockDataStruct{

    /**
     * 中文：借用的 WAL 定位对象；本包装器不持有或释放文件引用。
     * English: Borrowed WAL locator; this wrapper neither acquires nor releases a file reference.
     */
    private final DefaultMappedFile defaultMappedFile;
    /**
     * 中文：文件内零基逻辑块索引，来自 WAL 预留结果。
     * English: Zero-based logical block index within the file, obtained from WAL reservation.
     */
    private final int blockIndex;
    /**
     * 中文：源数据的零基字节偏移，不是 WAL 或 S3 偏移。
     * English: Zero-based source byte offset, not a WAL or S3 offset.
     */
    private final int fromOffset;
    /**
     * 中文：待复制的 Value 字节数，不含 WAL 头或对齐。
     * English: Value bytes to copy, excluding WAL headers and alignment.
     */
    private final int dataLen;
    /**
     * 中文：调用方拥有的源数组；须在 writeTo 期间保持内容稳定。
     * English: Caller-owned source array, which must remain stable during writeTo.
     */
    private final byte[] dataBytes;


    /**
     * 中文：仅保存源范围和逻辑定位；范围有效性主要在实际复制时检查。
     * English: Stores source range and logical location only; range validity is primarily checked by the actual copy.
     * @param defaultMappedFile 借用的 WAL 文件；English: Borrowed WAL file.
     * @param blockIndex 文件内逻辑块序号；English: Logical block index within the file.
     * @param dataBytes 借用的源数组；English: Borrowed source array.
     * @param fromOffset 源起始偏移，单位字节；English: Source start offset in bytes.
     * @param dataLen Value 长度，单位字节；English: Value length in bytes.
     */
    public HeapBlockDataStruct(DefaultMappedFile defaultMappedFile, int blockIndex, byte[] dataBytes, int fromOffset, int dataLen) {
        this.defaultMappedFile = defaultMappedFile;
        this.blockIndex = blockIndex;
        this.dataBytes = dataBytes;
        this.fromOffset=fromOffset;
        this.dataLen=dataLen;
    }

    /**
     * 中文：从源范围复制到 target 的 0 偏移；捕获的普通复制异常返回 false，是否重试由上层决定。
     * English: Copies the source range to target offset zero; caught ordinary copy exceptions return false, leaving retry policy to the caller.
     * @param target 可写目标切片，长度应至少为 dataLen；English: Writable target slice of at least dataLen bytes.
     * @return 成功为 true，捕获复制异常为 false；English: True on success, false on a caught copy exception.
     */
    @Override
    public boolean writeTo(MemorySegment target) {
        try {
            MemorySegment.copy(
                    dataBytes,
                    fromOffset,
                    target,
                    ValueLayout.JAVA_BYTE,
                    0,
                    dataLen
            );
            return true;
        }catch (Exception e){
            return false;
        }
    }



    /**
     * 中文：返回文件内逻辑块索引。
     * English: Returns the logical block index within the WAL file.
     * @return 零基逻辑索引；English: Zero-based logical index.
     */
    @Override
    public int getBlockIndex() {
        return blockIndex;
    }


    /**
     * 中文：返回本次 Value 长度。
     * English: Returns this value's length.
     * @return Value 字节数；English: Value byte count.
     */
    @Override
    public int getDataLen() {
        return dataLen;
    }

    /**
     * 中文：返回借用的 WAL 定位对象，不获取额外引用。
     * English: Returns the borrowed WAL locator without acquiring another reference.
     * @return 关联 WAL 文件；English: Associated WAL file.
     */
    @Override
    public DefaultMappedFile getDefaultMappedFile() {
        return defaultMappedFile;
    }
}
