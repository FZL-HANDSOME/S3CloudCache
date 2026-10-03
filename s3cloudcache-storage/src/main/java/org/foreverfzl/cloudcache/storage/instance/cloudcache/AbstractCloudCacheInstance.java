package org.foreverfzl.cloudcache.storage.instance.cloudcache;

/**
 * 中文：存储实例的生命周期边界；具体实现组织 Bucket、恢复任务、WAL、堆外池和上传客户端。
 * English: Lifecycle boundary for a storage instance; implementations coordinate buckets, recovery, WAL, native pools and upload clients.
 */
public abstract class AbstractCloudCacheInstance {

    /**
     * 中文：准备实例并启动恢复；具体实现可能异步恢复，返回不意味着历史 Block 已全部上传。
     * English: Prepares the instance and starts recovery, which may be asynchronous; return does not imply all historical blocks were uploaded.
     */
    public abstract void start();

    /**
     * 中文：停止接纳请求后按依赖顺序等待和释放；超时行为由实现定义，不能在活跃任务读取内存时强制卸载。
     * English: Stops admission and drains/releases resources in dependency order; implementations define timeouts and must not unmap live inputs.
     * @param walWriteWaitTime 中文：WAL 写入等待预算，毫秒，非负；English: nonnegative WAL-write wait budget in milliseconds.
     * @param blockWriteWaitTime 中文：物理 Block 写入等待预算，毫秒，非负；English: nonnegative block-write wait budget in milliseconds.
     * @param upLoadWaitTime 中文：上传等待预算，毫秒，非负；English: nonnegative upload wait budget in milliseconds.
     */
    public abstract void close(long walWriteWaitTime, long blockWriteWaitTime, long upLoadWaitTime);
}
