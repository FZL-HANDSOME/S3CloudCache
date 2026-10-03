package org.foreverfzl.cloudchache.common;

/**
 * 上层调用我们的API，我们返回该对象
 * 中文：一次写入的不可变位置结果；正常写入链路仅在整个 Block 上传并持久化确认后返回成功。
 * English: Immutable location result for one write; the normal write path reports success only after Block upload and durable acknowledgement.
 * 中文：失败结果不保证远端对象不存在；只有成功结果的 Key、offset、size 可作为已提交读取位置。
 * English: Failure does not prove remote absence; only successful Key, offset, and size form a committed read location.
 */
public class WriteResult {
    /**
     * 中文：所属 Block 的对象键，多条记录可共享；失败时可能为 null。
     * English: Object key shared by records in the Block; may be null on failure.
     */
    private final String s3Key;    // 1. 这批数据最终会归属于 S3 上的哪个大 Key（如 order/2026-06-05/xyz.block）
    /**
     * 中文：Value 在 S3 对象中的零起始字节偏移，不是 WAL 文件偏移。
     * English: Zero-based Value byte offset in S3, not a WAL-file offset.
     */
    private final long offset;        // 2. 这条业务数据在这个 5MB 大块内部的“绝对起始字节偏移量”
    /**
     * 中文：原始 Value 字节数，不包含协议头、WAL 对齐或 Block 尾部填充。
     * English: Original Value length excluding protocol headers, WAL alignment, and Block tail padding.
     */
    private final int size;          // 3. 这条业务数据的“总长度”）
    /**
     * 中文：本条记录所在 Block 是否被成功确认；构造器仅保存该标记，不自行访问 S3。
     * English: Whether the containing Block was acknowledged successfully; construction merely stores the flag.
     */
    private final boolean isSuccess;

    /**
     * 中文：构造结果快照；不校验坐标，不触发网络调用，也不获取或释放数据内存。
     * English: Constructs a result snapshot without validating coordinates, making network calls, or owning data memory.
     * @param s3Key 中文：所属 Block 对象键，失败时允许为空；English: Block object key, nullable on failure
     * @param offset 中文：Value 在对象内的字节偏移，失败路径可用 -1；English: in-object byte offset, possibly -1 on failure
     * @param size 中文：Value 字节数，失败路径可用 -1；English: Value byte count, possibly -1 on failure
     * @param isSuccess 中文：是否成功确认；English: whether the write was acknowledged successfully
     */
    public WriteResult(String s3Key, long offset, int size,boolean isSuccess) {
        this.s3Key = s3Key;
        this.offset = offset;
        this.size = size;
        this.isSuccess = isSuccess;
    }

    /**
     * 中文：取得数据所在对象键；读取前应先检查成功标记。
     * English: Returns the containing object key; check success before using it to read.
     * @return 中文：S3 对象键，失败时可为空；English: S3 key, possibly null on failure
     */
    public String getS3Key() {
        return s3Key;
    }

    /**
     * 中文：取得原始 Value 范围的起点，不需要再跳过 WAL 头。
     * English: Returns the Value range start; no WAL-header adjustment is needed.
     * @return 中文：对象内的字节偏移；English: byte offset within the object
     */
    public long getOffset() {
        return offset;
    }

    /**
     * 中文：取得从 offset 开始应读取的 Value 字节数。
     * English: Returns the number of Value bytes to read starting at offset.
     * @return 中文：原始 Value 长度；English: original Value length in bytes
     */
    public int getSize() {
        return size;
    }
    /**
     * 中文：检查结果是否为已确认成功，不通过该方法重新检查远端对象。
     * English: Checks the recorded success flag without revalidating the remote object.
     * @return 中文：已确认成功时为 true；English: true for acknowledged success
     */
    public boolean isSuccess() {
        return isSuccess;
    }
}
