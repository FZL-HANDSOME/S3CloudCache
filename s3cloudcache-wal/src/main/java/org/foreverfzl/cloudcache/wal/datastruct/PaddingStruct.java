package org.foreverfzl.cloudcache.wal.datastruct;

/**
 * Padding 墓碑协议元数据描述
 */
public final class PaddingStruct {

    // 🚨 独一无二的 Padding 魔数。读取流读到这里，如果 validateMagic 发现是这个值，说明是墓碑
    public static final int PADDING_MAGIC = 0x50444444;

    // Padding 头部固定占用的物理大小（单位：字节）
    public static final int PADDING_HEADER_SIZE = 4;

    /**
     * 私有构造，工具协议类禁止实例化，消灭堆内存开销
     */
    private PaddingStruct() {
        throw new UnsupportedOperationException("Protocol class inside storage engine cannot be instantiated");
    }
}
