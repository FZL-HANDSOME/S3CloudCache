package org.foreverfzl.cloudcache.storage.instance.bucket;

/**
 * 中文：单个 Bucket Writer 的准入生命周期，不代表任何具体 Block 的上传状态；只能向关闭方向推进。
 * English: Admission lifecycle of one bucket writer, not an individual block's upload state; transitions only move toward closure.
 */
public enum WriterState {

    /** 中文：允许登记新请求；English: accepts new requests. */
    RUNNING,

    /** 中文：拒绝新请求，已接纳请求仍需收尾；English: rejects new requests while accepted work drains. */
    CLOSING,

    /** 中文：Writer 恢复线程已退出；底层 Manager 由实例另行关闭；English: writer recovery worker has exited; the instance closes managers separately. */
    CLOSED

}
