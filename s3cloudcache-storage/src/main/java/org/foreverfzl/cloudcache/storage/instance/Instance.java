package org.foreverfzl.cloudcache.storage.instance;

import java.nio.ByteBuffer;

public interface Instance {
    ///照顾池化了 byte[] 的业务，避免了业务层的二次数组裁剪。
    public WriteResult write(String bucketName, String objectPrefix, byte[] data, int offset, int length);
    ///照顾普通低频业务，允许一次堆内到堆外的拷贝
    public WriteResult write(String bucketName, String objectPrefix, byte[] data);
    ///面向 Netty/网络层网关等极致吞吐场景，数据完全在堆外飞驰，JVM 堆内存冷眼旁观，实现真正的 零 JVM拷贝
    public WriteResult write(String bucketName, String objectPrefix, ByteBuffer buffer) ;




}
