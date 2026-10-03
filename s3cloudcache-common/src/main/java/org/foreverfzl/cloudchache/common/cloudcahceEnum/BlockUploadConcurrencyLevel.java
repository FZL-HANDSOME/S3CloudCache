package org.foreverfzl.cloudchache.common.cloudcahceEnum;


/**
 * S3 上传并发等级
 * <p>
 * 控制同时上传到对象存储的 Block 数量。
 * <p>
 * 主要影响：
 * 1. 网络带宽占用
 * 2. S3吞吐
 * 3. 堆外内存压力
 * 中文：每个 Bucket 上传并发上限的离散预设；实际上传许可由核心上传器管理。
 * English: Discrete per-Bucket upload limits; the core uploader manages the actual permits.
 */
public enum BlockUploadConcurrencyLevel {

    /**
     * 保守模式
     * <p>
     * 适合：
     * - 本地开发
     * - 低带宽机器
     * - 小规格服务器
     */
    /**
     * 中文：最多 4 个并发上传。
     * English: Up to 4 concurrent uploads.
     */
    LOW(4),

    /**
     * 普通模式
     * <p>
     * 推荐默认配置
     * <p>
     * 千兆网络:
     * 基本可以跑满
     */
    /**
     * 中文：最多 8 个并发上传，为 BucketConfig 默认值。
     * English: Up to 8 concurrent uploads, the BucketConfig default.
     */
    NORMAL(8),
    /**
     * 高吞吐模式
     * <p>
     * 适合：
     * - SSD
     * - 万兆网络
     * - 高性能S3服务
     */
    /**
     * 中文：最多 16 个并发上传。
     * English: Up to 16 concurrent uploads.
     */
    HIGH(16),

    /**
     * 极限模式
     * <p>
     * 适合：
     * - 分布式部署
     * - 专用上传节点
     */
    /**
     * 中文：最多 32 个并发上传。
     * English: Up to 32 concurrent uploads.
     */
    ULTRA(32);

    /**
     * 最大同时上传数量
     * 中文：许可数量，不是线程池大小，也不是数据字节数。
     * English: Permit count, not a thread-pool size or a byte count.
     */
    private final int concurrency;


    /**
     * 中文：保存当前枚举档位的并发许可数，不创建上传线程。
     * English: Stores this preset's permit count without creating upload threads.
     * @param concurrency 中文：正数并发上限；English: positive concurrency limit
     */
    BlockUploadConcurrencyLevel(int concurrency) {
        this.concurrency = concurrency;
    }


    /**
     * 中文：取得供 BucketConfig 使用的上传并发数。
     * English: Returns the upload concurrency value used by BucketConfig.
     * @return 中文：同时上传的 Block 数量上限；English: maximum concurrent Block uploads
     */
    public int getConcurrency() {
        return concurrency;
    }
}
