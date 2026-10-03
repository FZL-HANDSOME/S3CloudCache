package org.foreverfzl.cloudcache.wal.datastruct;

import java.lang.foreign.MemorySegment;

/**
 * 该接口代表WAL持久化协议
 */
/**
 * 中文：一条WAL记录的序列化契约：Magic(4B)、CRC32(4B)、Value长度(4B)、Value，记录总长度向上对齐4字节；不含Key。
 * English: Serialization contract: magic (4B), CRC32 (4B), value length (4B), and value, with total size rounded up to four bytes; no key is stored.
 */
public interface DataStruct {



    /** WAL磁盘化格式
     * ┌───────────────┬───────────────┬───────────────┬───────────────┬
     * │     Magic     │   Checksum    │   Value Len   │ Value Bytes   │
     * │   (4 Bytes)   │   (4 Bytes)   │   (4 Bytes)   │Variable Length│
     * └───────────────┴───────────────┴───────────────┴───────────────┴
     */

    // 固定魔数 0x53334343 (ASCII "S3CC")
    /**
     * 中文：正常记录的4字节魔数；当前序列化使用本机字节序。
     * English: Four-byte marker for a normal record; current serializers use native byte order.
     */
    public static final int MAGIC_NUMBER = 0x53334343;
    /**
     * 中文：显式结束标志；此常量不意味着每个写入路径都会主动写出结束标志。
     * English: Explicit end marker; defining it does not mean every writer path emits it.
     */
    public static final int END_MAGIC_NUMBER = 0x50444444;

    // 协议头部长度固定为 16 字节 (4 + 4 + 4)
    /**
     * 中文：更正上方历史注释：固定头是12字节，而非16字节；Value长度不包含该头和对齐填充。
     * English: Correction to the historical comment above: the fixed header is 12, not 16, bytes; value length excludes it and alignment padding.
     */
    public static final long HEADER_LENGTH = 12;

    /**
     * 将数据写入到target
     */
    /**
     * 中文：向调用方已独占的目标切片写记录；实现通常只复制到映射/PageCache，不负责 force。
     * English: Writes a record into the caller's exclusively reserved slice; implementations typically copy into mapping/page cache without forcing.
     *
     * @param target 中文：至少能容纳序列化记录且生命周期有效的目标；English: live target large enough for the serialized record
     */
    void writeTo(MemorySegment target);

    /**
     * 获取数据大小，4字节对齐
     */
    /**
     * 中文：返回WAL空间预留大小，包括头、Value及4字节对齐；不同于上传的Value字节数。
     * English: Returns reserved WAL size including header, value, and four-byte alignment; it differs from uploaded value bytes.
     *
     * @return 中文：对齐后的记录总字节数；English: aligned total record size in bytes
     */
    long getSerializedSize();

    /**
     * 中文：返回业务Value长度；expected/pageCache/finished计数使用此单位。
     * English: Returns business value length, the unit used by expected/pageCache/finished counters.
     *
     * @return 中文：Value字节数；English: number of value bytes
     */
    int getDataLen();


}
