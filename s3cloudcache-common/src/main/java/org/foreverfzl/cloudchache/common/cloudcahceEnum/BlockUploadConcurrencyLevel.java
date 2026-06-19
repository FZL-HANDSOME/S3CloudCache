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
    LOW(4),

    /**
     * 普通模式
     * <p>
     * 推荐默认配置
     * <p>
     * 千兆网络:
     * 基本可以跑满
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
    HIGH(16),

    /**
     * 极限模式
     * <p>
     * 适合：
     * - 分布式部署
     * - 专用上传节点
     */
    ULTRA(32);

    /**
     * 最大同时上传数量
     */
    private final int concurrency;


    BlockUploadConcurrencyLevel(int concurrency) {
        this.concurrency = concurrency;
    }


    public int getConcurrency() {
        return concurrency;
    }
}
