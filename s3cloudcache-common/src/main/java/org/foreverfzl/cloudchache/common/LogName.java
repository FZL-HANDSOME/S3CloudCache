package org.foreverfzl.cloudchache.common;

/**
 * 中文：各子系统共享的日志分类名称；这些字符串供 LoggerFactory 使用，不创建或配置日志后端。
 * English: Shared logger categories for subsystems; these strings identify loggers without creating or configuring a logging backend.
 */
public class LogName {

    /**
     * 中文：实例启动、恢复与关闭日志。
     * English: Instance startup, recovery, and shutdown logs.
     */
    public static final String CLOUD_CACHE_INSTANCE="CloudCacheInstance";
    /**
     * 中文：Bucket 接口层写入与生命周期日志。
     * English: Bucket-interface write and lifecycle logs.
     */
    public static final String BUCKET_INSTANCE="BucketInstance";
    /**
     * 中文：单个 WAL 映射文件操作日志。
     * English: Individual mapped-WAL file operation logs.
     */
    public static final String WAL_STORE_FILE = "StoreMappedFile";
    /**
     * 中文：物理 Block 分配与写入管理日志。
     * English: Physical Block allocation and write-management logs.
     */
    public static final String CACHE_BLOCK_MANAGER = "CacheBlockManager";
    /**
     * 中文：Block 上传和上传资源管理日志。
     * English: Block upload and upload-resource management logs.
     */
    public static final String CACHE_BLOCK_UPDATER = "CacheBlockUpdater";
    /**
     * 中文：实例级 Bucket WAL 调度日志。
     * English: Instance-level Bucket WAL coordination logs.
     */
    public static final String WAL_INSTANCE_BUCKET_MANAGER = "WalInstanceBucketManager";
    /**
     * 中文：Bucket 内 WAL 文件轮转与清理日志。
     * English: Per-Bucket WAL rotation and cleanup logs.
     */
    public static final String MAPPED_FILE_MANAGER = "MappedFileManager";
    /**
     * 中文：文件头元数据读写日志。
     * English: File-header metadata read/write logs.
     */
    public static final String FILE_META_INFO_UTIL = "FileMetaInfoUtil";
    /**
     * 中文：Bucket 配置元数据读写日志。
     * English: Bucket-configuration metadata read/write logs.
     */
    public static final String BUCKET_META_INFO_UTIL = "BucketMetaInfoUtil";
    /**
     * 中文：运行期恢复队列日志分类，字符串保留现有拼写。
     * English: Runtime recovery-queue category, retaining the existing spelling.
     */
    public static final String RECOVE_RQUEUE="recoveQueue";
    /**
     * 中文：待上传 Block 队列日志。
     * English: Pending Block upload-queue logs.
     */
    public static final String UPLOAD_QUEUE="UploadQueue";
    /**
     * 中文：失败 Block 死信队列日志。
     * English: Failed-Block dead-letter queue logs.
     */
    public static final String DEADDATA_QUEUE="DeadDataQueue";

}
