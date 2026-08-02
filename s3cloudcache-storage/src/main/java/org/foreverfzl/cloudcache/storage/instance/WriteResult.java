package org.foreverfzl.cloudcache.storage.instance;

/**
 * 上层调用我们的API，我们返回该对象
 */
public class WriteResult {
    private final String s3Key;    // 1. 这批数据最终会归属于 S3 上的哪个大 Key（如 order/2026-06-05/xyz.block）
    private final long offset;        // 2. 这条业务数据在这个 5MB 大块内部的“绝对起始字节偏移量”
    private final int size;          // 3. 这条业务数据的“总长度”）
    private final boolean isSuccess;

    public WriteResult(String s3Key, long offset, int size,boolean isSuccess) {
        this.s3Key = s3Key;
        this.offset = offset;
        this.size = size;
        this.isSuccess = isSuccess;
    }

    public String getS3Key() {
        return s3Key;
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }
    public boolean isSuccess() {
        return isSuccess;
    }
}
