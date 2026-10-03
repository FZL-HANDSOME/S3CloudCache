package org.foreverfzl.cloudchache.common.exception;

/**
 * 中文：标识堆外缓存核心层的非受检失败，与 WAL 或对外存储接口异常区分。
 * English: Identifies unchecked off-heap core failures separately from WAL and storage-interface failures.
 */
public class CoreException  extends CloudCacheException{
    /**
     * 中文：创建带核心层诊断信息的异常。
     * English: Creates an exception carrying core-layer diagnostics.
     * @param message 中文：错误说明，可为 null；English: error description, possibly null
     */
    public CoreException(String message) {
        super(message);
    }


    /**
     * 中文：包装核心层失败并保留底层原因，不在构造时重试。
     * English: Wraps a core failure and preserves its cause without retrying in the constructor.
     * @param message 中文：核心操作上下文；English: core-operation context
     * @param cause 中文：底层失败原因，可为 null；English: underlying cause, possibly null
     */
    public CoreException(
            String message,
            Throwable cause
    ) {
        super(message,cause);
    }
}
