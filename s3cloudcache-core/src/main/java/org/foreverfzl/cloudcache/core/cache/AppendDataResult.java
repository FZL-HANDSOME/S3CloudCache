package org.foreverfzl.cloudcache.core.cache;

/**
 * @param s3Key          1. 这批数据最终会归属于 S3 上的哪个大 Key（如 order/2026-06-05/xyz.block）
 * @param offset         2. 这条业务数据在这个 5MB 大块内部的“绝对起始字节偏移量”
 * @param size           3. 这条业务数据的“总长度”（Header + Payload）
 */
/**
 * 中文：表示一次物理 Block 追加的即时结果，不表示 S3 已提交；上方旧说明中的 size 在当前实现中仅计算 Value Bytes，不含 WAL 头，块容量也由配置决定而非固定 5MB。
 * English: Describes the immediate physical-block append result, not an S3 commit; size counts value bytes only rather than including headers, and block capacity is configured rather than fixed at 5 MB.
 * @param s3Key 对象键，失败时可为 null；English: Object key, possibly null on failure.
 * @param offset Value 在对象内的零基字节偏移，失败为 -1；English: Zero-based value offset in bytes, or -1 on failure.
 * @param size Value 字节数，失败为 -1；English: Value byte count, or -1 on failure.
 * @param result 本次物理追加是否成功；English: Whether this physical append succeeded.
 */
public record AppendDataResult(String s3Key, long offset, int size, boolean result) {

    /**
     * 中文：构造统一失败结果；文件定位参数只为调用接口保留，不写入该 record。
     * English: Creates the standard failure result; file-location arguments are retained by the API but are not stored in the record.
     * @param s3Key 已知对象键，可为 null；English: Known object key, or null.
     * @param fileFromOffset 调用方的 WAL 文件基址，本方法不使用；English: Caller WAL base offset, unused here.
     * @param blockIndex 调用方的块索引，本方法不使用；English: Caller block index, unused here.
     * @return offset/size 为 -1 且 result 为 false 的结果；English: A result with offset/size -1 and result false.
     */
    public static AppendDataResult fail(String s3Key,long fileFromOffset, int blockIndex) {
        return new AppendDataResult(s3Key, -1, -1, false);
    }
}
