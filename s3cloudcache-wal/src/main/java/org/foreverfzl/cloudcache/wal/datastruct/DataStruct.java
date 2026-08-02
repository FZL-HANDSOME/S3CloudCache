package org.foreverfzl.cloudcache.wal.datastruct;

import java.lang.foreign.MemorySegment;

/**
 * 该接口代表WAL持久化协议
 */
public interface DataStruct {



    /** WAL磁盘化格式
     * ┌───────────────┬───────────────┬───────────────┬───────────────┬
     * │     Magic     │   Checksum    │   Value Len   │ Value Bytes   │
     * │   (4 Bytes)   │   (4 Bytes)   │   (4 Bytes)   │Variable Length│
     * └───────────────┴───────────────┴───────────────┴───────────────┴
     */

    // 固定魔数 0x53334343 (ASCII "S3CC")
    public static final int MAGIC_NUMBER = 0x53334343;

    // 协议头部长度固定为 16 字节 (4 + 4 + 4 + 4)
    public static final long HEADER_LENGTH = 12;

    /**
     * 将数据写入到target
     */
    void writeTo(MemorySegment target);

    /**
     * 获取数据大小，4字节对齐
     */
    long getSerializedSize();

    int getDataLen();


}
