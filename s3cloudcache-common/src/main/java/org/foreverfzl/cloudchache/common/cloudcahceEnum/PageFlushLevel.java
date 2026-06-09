package org.foreverfzl.cloudchache.common.cloudcahceEnum;

/**
 * PageCache 刷盘级别
 * 控制 mmap 写入后的 force() 刷盘间隔，也是更新DefaultMappedFile的readPosition时间间隔
 */
public enum PageFlushLevel {


    /**
     * 10ms 刷盘一次
     * 延迟最低，磁盘压力最大
     */
    FAST_10_MS(10),


    /**
     * 20ms 刷盘一次
     * 低延迟模式
     */
    NORMAL_20_MS(20),


    /**
     * 40ms 刷盘一次
     * 平衡模式
     */
    BALANCE_40_MS(40),


    /**
     * 50ms 刷盘一次
     * 吞吐优先模式
     */
    THROUGHPUT_50_MS(50);


    /**
     * 刷盘时间间隔(ms)
     */
    private final long flushIntervalMs;


    PageFlushLevel(long flushIntervalMs) {
        this.flushIntervalMs = flushIntervalMs;
    }


    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }


    /**
     * 根据毫秒获取刷盘级别
     */
    public static PageFlushLevel fromMillis(long millis) {

        for (PageFlushLevel level : values()) {

            if (level.flushIntervalMs == millis) {
                return level;
            }
        }

        throw new IllegalArgumentException(
                "Unsupported flush interval: " + millis + "ms"
        );
    }
}
