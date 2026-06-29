package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudcache.storage.instance.Instance;
import org.foreverfzl.cloudcache.storage.instance.WriteResult;

import java.nio.ByteBuffer;

public abstract class AbstractBucketInstance implements Instance {

    /**
     * 照顾普通低频业务，允许一次堆内到堆外的拷贝
     * @param data
     * @return
     */
    public abstract WriteResult write(byte[] data);

    /**
     * 照顾池化了 byte[] 的业务，避免了业务层的二次数组裁剪。
     * @param data
     * @param offset
     * @param length
     * @return
     */
    public abstract WriteResult write(byte[] data, long offset, long length);

    /**
     * 面向 Netty/网络层网关等极致吞吐场景，数据完全在堆外飞驰，JVM 堆内存冷眼旁观，实现真正的 零 JVM拷贝
     *
     * @param buffer
     * @return
     */
    public abstract WriteResult write(ByteBuffer buffer);

    /**
     * 面向 Netty/网络层网关等极致吞吐场景，数据完全在堆外飞驰，JVM 堆内存冷眼旁观，实现真正的 零 JVM拷贝
     *
     * @param buffer
     * @return
     */
    public abstract WriteResult write(ByteBuffer buffer, long offset, long length);
}
