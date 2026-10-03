package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudchache.common.WriteResult;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * 中文：面向单个 Bucket 的追加接口；具体实现先复制进 WAL/物理 Block，再异步上传整块。
 * English: Append API for one bucket; the implementation copies into WAL/native blocks before asynchronously uploading a block.
 * 中文：成功 Future 才交付可用的对象位置；参数错误可异常完成，业务失败也可能返回 success=false，调用方必须检查两者。
 * English: Only a successful future result publishes a usable object location; callers must handle both exceptional completion and success=false.
 * 中文：调用可能因空闲 Block 不足而阻塞；源数据在调用返回前必须保持有效且不可被并发修改。
 * English: Calls may block under block-pool backpressure; keep source bytes alive and unchanged until the call returns.
 */
public interface Writer {

    /**
     * 照顾普通低频业务，允许一次堆内到堆外的拷贝
     * 中文：提交整个数组；不是一次调用对应一个 S3 对象，多个请求会合并到同一 Block。
     * English: Appends the entire array; multiple requests share a block rather than creating one S3 object per call.
     *
     * @param data 中文：非空且长度大于零的数组，所有权仍归调用方；English: nonempty caller-owned array.
     * @return 中文：上传及确认后的定位结果；English: future containing the location after upload and acknowledgment.
     */
    CompletableFuture<WriteResult> writeHeapData(byte[] data);

    /**
     * 照顾池化了 byte[] 的业务，避免了业务层的二次数组裁剪。
     * English: Accepts an array range without requiring the caller to allocate a trimmed array; internal WAL/block copies still occur.
     *
     * @param data 中文：非 null 的源数组；English: non-null source array.
     * @param offset 中文：数组起点字节偏移，非负；English: nonnegative byte offset from the array start.
     * @param length 中文：正字节数，范围必须完全位于数组内；English: positive byte count fully contained in the array.
     * @return 中文：整块提交后的位置 Future；English: location future completed after block commit.
     */
    CompletableFuture<WriteResult> writeHeapData(byte[] data, long offset, long length);

    /**
     * 面向 Netty/网络层网关等极致吞吐场景，数据完全在堆外飞驰，JVM 堆内存冷眼旁观，实现真正的 零 JVM拷贝
     * 中文：更正：避免的是显式中间 byte[]，并非操作系统零拷贝；仍复制进 WAL 和 Block，且实现未强制要求 direct buffer。
     * English: Clarification: avoids an explicit intermediate byte array, not OS-level copies; WAL/block copies remain and direct buffers are not enforced.
     *
     * @param buffer 中文：读取 [position, limit)，不推进 position；English: reads [position, limit) without advancing position.
     * @return 中文：整块提交结果，不转移 buffer 所有权；English: block-commit future; buffer ownership is not transferred.
     */
    CompletableFuture<WriteResult> writeOffHeapData(ByteBuffer buffer);

    /**
     * 面向 Netty/网络层网关等极致吞吐场景，数据完全在堆外飞驰，JVM 堆内存冷眼旁观，实现真正的 零 JVM拷贝
     * 中文：更正：使用共享底层内存的范围视图，仍有 WAL/Block 复制；不要在调用期间释放或修改源内存。
     * English: Clarification: uses a range view sharing the source storage; WAL/block copies remain, so do not release or mutate the source during the call.
     *
     * @param buffer 中文：非 null 的源 buffer，position/limit 保持不变；English: non-null source buffer with position/limit preserved.
     * @param offset 中文：相对当前 position 的非负字节偏移；English: nonnegative byte offset relative to current position.
     * @param length 中文：正字节数，offset+length 不超过 remaining；English: positive byte count within remaining after offset.
     * @return 中文：整块提交结果 Future；English: future for the block-commit result.
     */
    CompletableFuture<WriteResult> writeOffHeapData(ByteBuffer buffer, long offset, long length);
}
