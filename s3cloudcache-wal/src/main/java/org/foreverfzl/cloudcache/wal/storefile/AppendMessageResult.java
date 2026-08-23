package org.foreverfzl.cloudcache.wal.storefile;

/**
 * 数据追加写入操作的结果封装类。
 * 包含写入状态、写入偏移量、写入字节数等信息。
 */
public class AppendMessageResult {

    /**
     * 写入状态
     */
    private final AppendStatus status;

    /**
     * 写入的文件引用
     */
    private final DefaultMappedFile defaultMappedFile;

    /**
     * 写入的目标文件名
     */
    private final long fileFromOffset;

    /**
     * 该数据在改文件的哪逻辑Block中
     */
    private int logicalIndex;

    /**
     * 写入时的时间戳
     */
    private final long storeTimestamp;


    public AppendMessageResult(DefaultMappedFile defaultMappedFile, AppendStatus status, long storeTimestamp, long fileFromOffset) {
        this.defaultMappedFile = defaultMappedFile;
        this.status = status;
        this.storeTimestamp = storeTimestamp;
        this.fileFromOffset = fileFromOffset;
        logicalIndex = -1;
    }


    /**
     * 创建一个表示失败的结果（不携带偏移和字节数信息）
     */
    public static AppendMessageResult fail(DefaultMappedFile defaultMappedFile, AppendStatus status, long fileFromOffset) {
        return new AppendMessageResult(defaultMappedFile, status, System.currentTimeMillis(), fileFromOffset);
    }


    /**
     * 追加写入的状态枚举
     */
    public enum AppendStatus {
        /**
         * 写入成功，继续后续业务或返回 ACK。
         */
        PUT_OK,
        /**
         * 文件剩余空间不足，已写满
         */
        END_OF_FILE,
        /**
         * 参数异常（如 walDataStruct 为空）
         */
        INVALID_ARGUMENT,
        /**
         * 写入过程中发生未知错误
         */
        WRITER_FAILED,
        /**
         * 文件关闭
         */
        FILE_CLOSED,
    }

    /**
     * 判断写入是否成功
     */
    public boolean isOk() {
        return this.status == AppendStatus.PUT_OK;
    }

    public AppendStatus getStatus() {
        return status;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public int getLogicalIndex() {
        return logicalIndex;
    }

    public void setLogicalIndex(int logicalIndex) {
        this.logicalIndex = logicalIndex;
    }

    public DefaultMappedFile getDefaultMappedFile() {
        return defaultMappedFile;
    }

    @Override
    public String toString() {
        return "AppendMessageResult{" +
                "status=" + status +
                ", fileFromOffset=" + fileFromOffset +
                ", logicalIndex=" + logicalIndex +
                ", storeTimestamp=" + storeTimestamp +
                '}';
    }
}
