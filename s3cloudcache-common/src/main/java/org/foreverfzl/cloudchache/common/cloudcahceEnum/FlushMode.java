package org.foreverfzl.cloudchache.common.cloudcahceEnum;

/**
 * 中文：刷盘策略名称的预留枚举；当前 BucketConfig 和 WAL 写入链路未使用该枚举选择行为。
 * English: Reserved flush-policy names; current BucketConfig and WAL writes do not select behavior through this enum.
 */
public enum FlushMode {
    /**
     * 完全依赖 Linux Page Cache
     * 中文：异步策略标识，枚举常量本身不会执行刷盘。
     * English: Asynchronous-policy marker; the constant itself does not flush data.
     */
    ASYNC,

    /**
     * 每次 append 后 force()
     * 中文：同步策略标识，并非当前写入 API 的独立开关。
     * English: Synchronous-policy marker, not a separate switch on the current write API.
     */
    SYNC,

    /**
     * 后台线程定时 force()
     * 中文：定时策略标识，不负责创建或调度后台线程。
     * English: Timed-policy marker; it neither creates nor schedules background threads.
     */
    TIMED
}
