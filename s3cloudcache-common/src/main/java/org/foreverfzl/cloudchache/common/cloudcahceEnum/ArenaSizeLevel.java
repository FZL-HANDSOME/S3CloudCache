package org.foreverfzl.cloudchache.common.cloudcahceEnum;

/**
 * 全局堆外缓冲区（Global Arena）总容量的黄金配置档位
 * 严格按照 2 的幂次方规划物理空间，平衡单机多物理节点下的资源消耗与高并发抗压能力。
 *
 * @author 范泽麟
 */
public enum ArenaSizeLevel {

    /** * 轻量级（512MB）：适合测试环境、侧边栏小服务、或者单条数据极小的边缘业务。
     */
    LIGHTWEIGHT(512 * 1024 * 1024L),

    /** * 通用标准型（1GB）：【推荐默认值】性能、安全、资源消耗的完美平衡。
     * 在单机 5 万 TPS 狂暴冲刷下，能死硬抗 40 秒以上的网络/云端抖动。
     */
    STANDARD(1024 * 1024 * 1024L),

    /** * 高吞吐型 - 中（2GB）：适合高并发核心吞吐节点，提供翻倍的安全缓冲气囊。
     */
    HIGH_THROUGHPUT_2G(2L * 1024 * 1024 * 1024L),

    /** * 极致吞吐型 - 大（4GB）：适合大数据核心吞吐节点。例如每天要喷几十个 T 数据的日志聚合网关。
     */
    HIGH_THROUGHPUT_4G(4L * 1024 * 1024 * 1024L);

    // 堆外内存极大，必须使用 long 类型防止 int 越界溢出
    private final long bytes;

    ArenaSizeLevel(long bytes) {
        this.bytes = bytes;
    }

    /**
     * 获取当前档位对应的具体字节数（Byte）
     */
    public long getBytes() {
        return bytes;
    }

    /**
     * 获取当前档位对应的兆字节数（MB），用于日志和监控打印
     */
    public long toMegabytes() {
        return bytes / (1024 * 1024);
    }


}
