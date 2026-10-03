package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudchache.common.WriteResult;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * 中文：Writer 的抽象扩展点，增加失败 Block 的人工恢复入口；自身不持有线程或存储资源。
 * English: Abstract Writer extension adding manual recovery of failed blocks; this base class owns no threads or storage resources.
 * 中文：写入范围、源内存生命周期及 Future 语义见 Writer；实现负责准入控制和资源关闭协调。
 * English: See Writer for ranges, source lifetimes and future semantics; implementations coordinate admission and resource shutdown.
 */
public abstract class AbstractBucketWriter implements Writer {

    /**
     * 照顾普通低频业务，允许一次堆内到堆外的拷贝
     * English: Full-array append for ordinary callers; concrete implementations copy into WAL and block storage.
     *
     * @param data 中文：调用返回前保持不变的非空数组；English: nonempty array unchanged until the call returns.
     * @return 中文：整块上传确认结果；English: future for the block upload acknowledgment.
     */
    public abstract CompletableFuture<WriteResult> writeHeapData(byte[] data);

    /**
     * 照顾池化了 byte[] 的业务，避免了业务层的二次数组裁剪。
     * English: Range append avoids allocating a trimmed array in the caller, but is not a zero-copy storage operation.
     *
     * @param data 中文：非 null 源数组；English: non-null source array.
     * @param offset 中文：从数组起点计算的非负字节偏移；English: nonnegative byte offset from the array start.
     * @param length 中文：数组范围内的正字节数；English: positive byte count within the array.
     * @return 中文：异步提交结果；English: asynchronous commit result.
     */
    public abstract CompletableFuture<WriteResult> writeHeapData(byte[] data, long offset, long length);

    /**
     * 面向 Netty/网络层网关等极致吞吐场景，数据完全在堆外飞驰，JVM 堆内存冷眼旁观，实现真正的 零 JVM拷贝
     * 中文：更正：不要求中间 byte[]，但 WAL 与物理 Block 仍需复制；具体实现也接受可访问的堆内 buffer。
     * English: Clarification: no intermediate byte array is required, but WAL/native-block copies remain; accessible heap buffers also work.
     *
     * @param buffer 中文：非空剩余范围 [position, limit)，由调用方拥有；English: caller-owned buffer with a nonempty remaining range.
     * @return 中文：整块提交结果 Future；English: block-commit result future.
     */
    public abstract CompletableFuture<WriteResult> writeOffHeapData(ByteBuffer buffer);

    /**
     * 将堆外数据 buffer 的 [position+offset, position+offset+length) 部分上传到 S3
     * English: Appends the range relative to current position without advancing the source buffer or taking ownership.
     * @param buffer 堆外数据；English: caller-owned source buffer, also accepts accessible heap-backed buffers.
     * @param offset 相对于当前 position 的偏移量（必须 >= 0）；English: nonnegative byte offset relative to position.
     * @param length 要上传的数据长度；English: positive byte length fitting remaining after offset.
     * 注意：不会推进 buffer 的 position
     * 示例：若 buffer.position()=10, offset=5, length=20，则上传 [15, 35)
     * English: Offset is nonnegative and length is positive; both must fit remaining. Position 10, offset 5 and length 20 select [15, 35).
     * @return 中文：上传确认后的位置 Future；English: future for the acknowledged object location.
     */
    public abstract CompletableFuture<WriteResult> writeOffHeapData(ByteBuffer buffer, long offset, long length);

    /**
     * 监听死信队列中的数据
     * 中文：阻塞取得一个失败 Block 的只读快照；消费失败通知不代表已经修复或确认 WAL。
     * English: Blocks for a read-only snapshot of a failed block; consuming a notification neither repairs nor acknowledges WAL.
     * @return 中文：调用方负责关闭的 Reader，上传成功后另行显式 ack；English: caller-owned reader to close; acknowledge separately after successful upload.
     * @throws InterruptedException 中文：等待失败队列时被中断；English: interrupted while waiting for the failure queue.
     */
    public abstract MappedFileReader getUpLoadFailedBlockInfo() throws InterruptedException;


}
