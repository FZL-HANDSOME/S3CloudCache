package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudchache.common.WriteResult;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractBucketWriter implements Writer {

    /**
     * 照顾普通低频业务，允许一次堆内到堆外的拷贝
     *
     * @param data
     * @return
     */
    public abstract CompletableFuture<WriteResult> writeHeapData(byte[] data);

    /**
     * 照顾池化了 byte[] 的业务，避免了业务层的二次数组裁剪。
     *
     * @param data
     * @param offset
     * @param length
     * @return
     */
    public abstract CompletableFuture<WriteResult> writeHeapData(byte[] data, long offset, long length);

    /**
     * 面向 Netty/网络层网关等极致吞吐场景，数据完全在堆外飞驰，JVM 堆内存冷眼旁观，实现真正的 零 JVM拷贝
     *
     * @param buffer
     * @return
     */
    public abstract CompletableFuture<WriteResult> writeOffHeapData(ByteBuffer buffer);

    /**
     * 将堆外数据 buffer 的 [position+offset, position+offset+length) 部分上传到 S3
     * @param buffer 堆外数据
     * @param offset 相对于当前 position 的偏移量（必须 >= 0）
     * @param length 要上传的数据长度
     * 注意：不会推进 buffer 的 position
     * 示例：若 buffer.position()=10, offset=5, length=20，则上传 [15, 35)
     */
    public abstract CompletableFuture<WriteResult> writeOffHeapData(ByteBuffer buffer, long offset, long length);

    /**
     * 监听死信队列中的数据
     */
    public abstract MappedFileReader getUpLoadFailedBlockInfo() throws InterruptedException;


}
