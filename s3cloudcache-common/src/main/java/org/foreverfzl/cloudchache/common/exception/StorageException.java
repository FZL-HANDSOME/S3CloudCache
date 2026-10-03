package org.foreverfzl.cloudchache.common.exception;

/**
 * 中文：标识存储接口层的非受检失败；异常类型本身不意味着 WAL 已删除或远端一定没有对象。
 * English: Identifies unchecked storage-interface failures; the type alone implies neither WAL deletion nor remote absence.
 */
public class StorageException extends CloudCacheException{

    /**
     * 中文：创建包含存储操作诊断信息的异常。
     * English: Creates an exception carrying storage-operation diagnostics.
     * @param message 中文：错误说明，可为 null；English: error description, possibly null
     */
    public StorageException(String message) {
        super(message);
    }


    /**
     * 中文：保留存储操作上下文和最初失败的异常链。
     * English: Preserves storage-operation context and the original cause chain.
     * @param message 中文：存储操作上下文；English: storage-operation context
     * @param cause 中文：底层异常，可为 null；English: underlying cause, possibly null
     */
    public StorageException(String message, Throwable cause) {
        super(message,cause);
    }
}
