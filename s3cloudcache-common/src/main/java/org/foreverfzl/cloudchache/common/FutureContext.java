package org.foreverfzl.cloudchache.common;

import java.util.concurrent.CompletableFuture;

/**
 * 调用Write方法的上下文对象
 * 中文：关联一次原始 WAL 记录与其异步结果；恢复时保留同一 Future，并可重新确定物理 Value 偏移。
 * English: Associates an original WAL record with its result; recovery retains its Future while rebuilding the physical Value offset.
 * 中文：字段可变且没有独立同步，发布、更新和读取必须遵守所属 Block 元数据的生命周期与锁约束。
 * English: Fields are mutable without internal synchronization; publication and access follow the owning Block metadata's lifecycle and locks.
 */
public final class FutureContext {
    /**
     * 中文：记录在逻辑 WAL 块内的协议记录起点，用于恢复时匹配原 Future；不是 S3 Value 偏移。
     * English: Record start within the logical WAL block, used to match the original Future during recovery, not its S3 Value offset.
     */
    private long walRecordId;
    /**
     * 中文：所属物理 Block 的 S3 对象键，绑定前可为 null。
     * English: S3 object key of the physical Block; may be null before binding.
     */
    private String s3Key;
    /**
     * 中文：Value 在最终对象内的字节偏移；-1 表示尚未取得物理位置，不含 WAL 头和对齐。
     * English: Value byte offset in the final object; -1 means unassigned, excluding WAL headers and alignment.
     */
    private long physicalOffset = -1;
    /**
     * 中文：原始 Value 字节数，不是含 12 字节协议头的 WAL 序列化长度。
     * English: Original Value bytes, not serialized WAL length with its 12-byte header.
     */
    private int size;
    /**
     * 中文：调用方观察的同一个完成句柄；上下文保存引用，不复制 Future 或负责释放存储资源。
     * English: The caller's completion handle; the context retains it without copying or owning storage resources.
     */
    private CompletableFuture<WriteResult> future;


    /**
     * 中文：保存写入请求的完成句柄，位置字段由 WAL 和物理写入阶段随后填充。
     * English: Retains the completion handle; WAL and physical-write stages fill in the location later.
     * @param future 中文：对应一次写入的 Future，由调用方传入，本构造器不校验 null；English: write Future supplied by the caller; null is not checked here
     */
    public FutureContext(CompletableFuture<WriteResult> future) {
        this.future = future;
    }

    /**
     * 中文：记录原 WAL 协议记录的身份，使恢复重放能找到原来的完成句柄。
     * English: Records the original WAL record identity so replay can find its completion handle.
     * @param walRecordId 中文：逻辑块内的 WAL 记录字节起点；English: WAL record byte start within the logical block
     */
    public void setWalRecordId(long walRecordId) {
        this.walRecordId = walRecordId;
    }

    /**
     * 中文：关联承载本条 Value 的 Block 对象键，不在此执行上传。
     * English: Associates the Block object key carrying this Value without performing an upload.
     * @param s3Key 中文：目标 S3 对象键；English: target S3 object key
     */
    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }

    /**
     * 中文：保存当前构建版本中 Value 的物理偏移，未确认 Block 在恢复时可重新排列。
     * English: Stores the Value offset for the current build; recovery may reorder an unacknowledged Block.
     * @param physicalOffset 中文：纯 Value 对象中的字节偏移；English: byte offset in the Value-only object
     */
    public void setPhysicalOffset(long physicalOffset) {
        this.physicalOffset = physicalOffset;
    }

    /**
     * 中文：记录调用方 Value 的长度，供最终结果定位数据范围。
     * English: Records the caller's Value length for locating its range in the final result.
     * @param size 中文：Value 字节数；English: Value length in bytes
     */
    public void setSize(int size) {
        this.size = size;
    }


    /**
     * 中文：取得恢复匹配使用的 WAL 记录起点。
     * English: Returns the WAL record start used for replay matching.
     * @return 中文：块内 WAL 字节位置，未赋值时为 0；English: in-block WAL byte position, initially zero
     */
    public long getWalRecordId() {
        return walRecordId;
    }

    /**
     * 中文：读取当前绑定的对象键；此值存在本身不代表上传已确认。
     * English: Reads the currently bound object key; its presence alone does not acknowledge an upload.
     * @return 中文：对象键或尚未绑定时的 null；English: object key, or null before binding
     */
    public String getS3Key() {
        return s3Key;
    }

    /**
     * 中文：取得 Value 在当前物理 Block 中的偏移。
     * English: Returns the Value offset in the current physical Block.
     * @return 中文：字节偏移，未赋值时为 -1；English: byte offset, or -1 before assignment
     */
    public long getPhysicalOffset() {
        return physicalOffset;
    }

    /**
     * 中文：取得用于结果范围的原始数据长度。
     * English: Returns the original data length used in the result range.
     * @return 中文：Value 字节数，未赋值时为 0；English: Value byte count, initially zero
     */
    public int getSize() {
        return size;
    }

    /**
     * 中文：返回原始完成句柄，不创建新 Future，也不等待上传。
     * English: Returns the original handle without creating a Future or waiting for upload.
     * @return 中文：构造时保存的 Future 引用；English: the Future reference retained at construction
     */
    public CompletableFuture<WriteResult> getFuture() {
        return future;
    }
}
