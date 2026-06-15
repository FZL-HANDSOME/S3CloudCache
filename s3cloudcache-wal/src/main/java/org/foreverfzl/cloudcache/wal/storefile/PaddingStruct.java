package org.foreverfzl.cloudcache.wal.storefile;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

/**
 * Padding 墓碑协议元数据描述。
 * <p>
 * 采用极简的 8 字节紧凑结构：
 * <ul>
 * <li>4 字节：PADDING_MAGIC (0x50414444 -> ASCII "PADD")</li>
 * <li>4 字节：remainingInBlock (当前块剩下的总残渣长度，用于顺序读取器直接跳过)</li>
 * </ul>
 * </p>
 */
public final class PaddingStruct {

    // 🚨 独一无二的 Padding 魔数。读取流读到这里，如果 validateMagic 发现是这个值，说明是墓碑
    public static final int PADDING_MAGIC = 0x50414444;

    // Padding 头部固定占用的物理大小（单位：字节）
    public static final int PADDING_HEADER_SIZE = 8;

    // 采用 Java 21+ 标准的 MemoryLayout 描述该结构体的底层物理摆放，便于后续高级特性的扩展
    public static final StructLayout PADDING_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.JAVA_INT.withName("magic"),
            ValueLayout.JAVA_INT.withName("length")
    );

    /**
     * 私有构造，工具协议类禁止实例化，消灭堆内存开销
     */
    private PaddingStruct() {
        throw new UnsupportedOperationException("Protocol class inside storage engine cannot be instantiated");
    }
}
