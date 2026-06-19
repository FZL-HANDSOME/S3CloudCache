package org.foreverfzl.cloudcache.core.cache;

/**
 * @param s3Key  1. 这批数据最终会归属于 S3 上的哪个大 Key（如 order/2026-06-05/xyz.block）
 * @param offset 2. 这条业务数据在这个 5MB 大块内部的“绝对起始字节偏移量”
 * @param size   3. 这条业务数据的“总长度”（Header + Payload）
 */
public record AppendDataResult(String s3Key, long offset, int size, boolean result) {

    public static AppendDataResult fail() {
        return new AppendDataResult(null, -1, -1, false);
    }
}
