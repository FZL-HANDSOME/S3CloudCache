package org.foreverfzl.cloudcache.wal.storefile;

/**
 * 数据追加写入操作的结果封装类。
 * 包含写入状态、写入偏移量、写入字节数等信息。
 */
public class AppendMessageResult {

    /**
     * 追加写入的状态枚举
     */
    public enum AppendStatus {
        /**
         * 写入成功
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
        UNKNOWN_ERROR
    }

    /**
     * 写入状态
     */
    private final AppendStatus status;

    /**
     * 数据写入的起始偏移量（相对于文件起始）
     */
    private final long wroteOffset;

    /**
     * 实际写入的字节数
     */
    private final int wroteBytes;

    /**
     * 写入时的时间戳
     */
    private final long storeTimestamp;

    /**
     * 写入的目标文件名
     */
    private final String fileName;


    public AppendMessageResult(AppendStatus status, String fileName) {
        this.status = status;
        this.fileName = fileName;
        wroteOffset = -1;
        wroteBytes = -1;
        storeTimestamp = -1;
    }

    public AppendMessageResult(AppendStatus status, long wroteOffset, int wroteBytes,
                               long storeTimestamp, String fileName) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.storeTimestamp = storeTimestamp;
        this.fileName = fileName;
    }

    /**
     * 创建一个表示失败的结果（不携带偏移和字节数信息）
     */
    public static AppendMessageResult fail(AppendStatus status, String fileName) {
        return new AppendMessageResult(status, -1, 0, System.currentTimeMillis(), fileName);
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

    public long getWroteOffset() {
        return wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public String getFileName() {
        return fileName;
    }

    @Override
    public String toString() {
        return "AppendMessageResult{" +
                "status=" + status +
                ", wroteOffset=" + wroteOffset +
                ", wroteBytes=" + wroteBytes +
                ", storeTimestamp=" + storeTimestamp +
                ", fileName='" + fileName + '\'' +
                '}';
    }
}
