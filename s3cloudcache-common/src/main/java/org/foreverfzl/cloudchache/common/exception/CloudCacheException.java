package org.foreverfzl.cloudchache.common.exception;

/**
 * 中文：CloudCache 模块异常的非受检基类；保留错误原因供调用方决定重试、停止或人工恢复。
 * English: Unchecked base exception for CloudCache modules, retaining causes for caller-directed retry, shutdown, or repair.
 */
public class CloudCacheException extends RuntimeException {
    /**
     * 中文：创建没有消息和原因的异常，不附带任何自动恢复动作。
     * English: Creates an exception without a message or cause and without automatic recovery.
     */
    public CloudCacheException() {
        super();
    }


    /**
     * 中文：保存调用处提供的诊断消息。
     * English: Retains the diagnostic message provided at the failure site.
     * @param message 中文：错误描述，可为 null；English: error description, possibly null
     */
    public CloudCacheException(String message) {
        super(message);
    }


    /**
     * 中文：在添加模块上下文的同时保留底层失败原因。
     * English: Adds module context while preserving the underlying failure cause.
     * @param message 中文：上下文描述，可为 null；English: contextual message, possibly null
     * @param cause 中文：底层异常，可为 null；English: underlying cause, possibly null
     */
    public CloudCacheException(String message, Throwable cause) {
        super(message, cause);
    }


    /**
     * 中文：包装底层原因，消息采用 RuntimeException 的原因派生规则。
     * English: Wraps a cause using RuntimeException's cause-derived message behavior.
     * @param cause 中文：原始失败原因，可为 null；English: original cause, possibly null
     */
    public CloudCacheException(Throwable cause) {
        super(cause);
    }
}
