package org.foreverfzl.cloudchache.common.exception;

/**
 * 中文：WAL 文件、持久元数据及恢复校验失败的非受检异常；是否可重试由具体失败点决定。
 * English: Unchecked exception for WAL files, durable metadata, and recovery validation; retryability depends on the failure site.
 */
public class WalException extends CloudCacheException {
    /**
     * 中文：创建说明 WAL 失败原因的异常。
     * English: Creates an exception describing a WAL failure.
     * @param message 中文：诊断信息，可为 null；English: diagnostic message, possibly null
     */
    public WalException(String message) {
        super(message);
    }


    /**
     * 中文：添加 WAL 操作上下文并保留底层 I/O 或校验异常。
     * English: Adds WAL-operation context while retaining the underlying I/O or validation cause.
     * @param message 中文：WAL 操作上下文；English: WAL-operation context
     * @param cause 中文：底层失败原因，可为 null；English: underlying cause, possibly null
     */
    public WalException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }
}
