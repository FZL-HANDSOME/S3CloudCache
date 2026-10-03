package org.foreverfzl.cloudchache.common.cloudcahceEnum;

/**
 * 全局堆外缓冲区（Global Arena）总容量的黄金配置档位
 * 严格按照 2 的幂次方规划物理空间，平衡单机多物理节点下的资源消耗与高并发抗压能力。
 * 中文：配置单个 Bucket 的物理缓存池容量；枚举只提供数值，不预分配内存或保证吞吐量。
 * English: Configures one Bucket's physical cache pool; presets only provide values, not allocation or throughput guarantees.
 */
public enum CacheSizeLevel {

    /**
     * 轻量级（256MB）：适合测试环境、侧边栏小服务、或者单条数据极小的边缘业务。
     */
    /**
     * 中文：256 MiB，BucketConfig 默认档位。
     * English: 256 MiB, the BucketConfig default.
     */
    TINE(256 * 1024 * 1024L),

    /**
     * 轻量级（512MB）：适合测试环境、侧边栏小服务、或者单条数据极小的边缘业务。
     */
    /**
     * 中文：512 MiB 缓存容量。
     * English: 512 MiB of cache capacity.
     */
    LIGHTWEIGHT(512 * 1024 * 1024L),

    /**
     * 通用标准型（1GB）：【推荐默认值】性能、安全、资源消耗的完美平衡。
     * 在单机 5 万 TPS 狂暴冲刷下，能死硬抗 40 秒以上的网络/云端抖动。
     */
    /**
     * 中文：1 GiB 缓存容量。
     * English: 1 GiB of cache capacity.
     */
    STANDARD(1024 * 1024 * 1024L),

    /**
     * 高吞吐型 - 中（2GB）：适合高并发核心吞吐节点，提供翻倍的安全缓冲气囊。
     */
    /**
     * 中文：2 GiB 缓存容量，超出正 int 的字节范围。
     * English: 2 GiB, exceeding a positive int byte count.
     */
    HIGH_THROUGHPUT_2G(2L * 1024 * 1024 * 1024L);


    // 堆外内存极大，必须使用 long 类型防止 int 越界溢出
    /**
     * 中文：整个物理缓存池的字节容量。
     * English: Byte capacity of the entire physical cache pool.
     */
    private final long bytes;

    /**
     * 中文：保存枚举常量指定的容量值。
     * English: Stores the capacity declared by the enum constant.
     * @param bytes 中文：缓存总字节数；English: total cache capacity in bytes
     */
    CacheSizeLevel(long bytes) {
        this.bytes = bytes;
    }

    /**
     * 获取当前档位对应的具体字节数（Byte）
     * 中文：返回容量配置的原始 long 字节值。
     * English: Returns the raw long byte count of this capacity preset.
     * @return 中文：缓存池总字节数；English: total cache-pool bytes
     */
    public long getBytes() {
        return bytes;
    }

    /**
     * 获取当前档位对应的兆字节数（MB），用于日志和监控打印
     * 中文：按 1024 × 1024 字节换算，结果单位为 MiB。
     * English: Converts using 1024 × 1024 bytes, so the result is in MiB.
     * @return 中文：二进制兆字节数；English: capacity in mebibytes
     */
    public long toMegabytes() {
        return bytes / (1024 * 1024);
    }


}
