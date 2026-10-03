package org.foreverfzl.cloudcache.core.datastruct;

import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;

import java.lang.foreign.MemorySegment;

/**
 * CoreBlock协议格式
 */
/**
 * 中文：描述已定位到 WAL 逻辑块的 Value 复制操作；物理缓存只存 Value，不复制 WAL 协议头。
 * English: Describes a value copy associated with a WAL logical block; the physical cache stores values without WAL headers.
 */
public interface BlockDataStruct {

    /**
     * 中文：将该记录的 Value 复制到目标段起点；实现报告复制结果，不负责引用、计数或提交。
     * English: Copies the record value to the beginning of the target segment; implementations report copying only, not reference management, accounting or commitment.
     * @param target 调用方持有有效租约的目标切片；English: Target slice covered by the caller's valid lease.
     * @return 复制是否成功；English: Whether copying succeeded.
     */
    boolean writeTo(MemorySegment target);


    /**
     * 中文：取得记录所属的文件内逻辑块序号。
     * English: Gets the record's logical block index within its WAL file.
     * @return 零基逻辑块索引；English: Zero-based logical block index.
     */
    int getBlockIndex();

    /**
     * 中文：取得仅 Value 的字节数。
     * English: Gets the value-only byte count.
     * @return 不含 WAL 头和对齐填充的长度；English: Length excluding WAL headers and alignment padding.
     */
    int getDataLen();

    /**
     * 中文：取得定位该记录的 WAL 文件对象；返回值不自动增加文件引用。
     * English: Gets the associated WAL file without automatically acquiring a file reference.
     * @return 借用的 WAL 文件对象；English: Borrowed WAL file object.
     */
    DefaultMappedFile getDefaultMappedFile();



}
