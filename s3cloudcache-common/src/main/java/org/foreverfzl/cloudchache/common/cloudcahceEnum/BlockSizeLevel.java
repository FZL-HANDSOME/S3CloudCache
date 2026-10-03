package org.foreverfzl.cloudchache.common.cloudcahceEnum;

/**
 * 中文：单个逻辑 WAL 块和物理缓存块的容量预设；数值以字节计，均为 2 的幂。
 * English: Byte-capacity presets for one logical WAL block and one physical cache block; all are powers of two.
 */
public enum BlockSizeLevel {

    /**
     * 中文：2 MiB 块。
     * English: A 2 MiB block.
     */
    TINY(2 * 1024 * 1024),
    /**
     * 中文：4 MiB 块，BucketConfig 的默认档位。
     * English: A 4 MiB block, the BucketConfig default.
     */
    SMALL(4 * 1024 * 1024),
    /**
     * 中文：8 MiB 块。
     * English: An 8 MiB block.
     */
    MEDIUM(8 * 1024 * 1024),
    /**
     * 中文：16 MiB 块。
     * English: A 16 MiB block.
     */
    LARGE(16 * 1024 * 1024),
    /**
     * 中文：32 MiB 块。
     * English: A 32 MiB block.
     */
    ULTRA(32 * 1024 * 1024);

    /**
     * 中文：单块容量，不是一次业务写入的 Value 长度。
     * English: Block capacity, not the Value length of one write.
     */
    private final int bytes;

    /**
     * 中文：保存枚举声明提供的单块字节容量，不在此处分配内存。
     * English: Stores the declared block capacity without allocating memory.
     * @param bytes 中文：单块字节数；English: bytes per block
     */
    BlockSizeLevel(int bytes) {
        this.bytes = bytes;
    }

    /**
     * 中文：取得可写入 BucketConfig 的单块容量值。
     * English: Returns the block capacity suitable for BucketConfig.
     * @return 中文：字节数；English: capacity in bytes
     */
    public int getBytes() {
        return bytes;
    }
}
