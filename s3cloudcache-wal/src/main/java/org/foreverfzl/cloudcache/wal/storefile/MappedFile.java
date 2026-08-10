package org.foreverfzl.cloudcache.wal.storefile;

import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;

import java.nio.channels.FileChannel;

public interface MappedFile {

    /**
     * 文件预热，并且根据配置选择是否锁定预热PageCache
     *
     * @param pages
     */
    public void warm(int pages, boolean isLockMemory);

    /**
     *
     * @param dataStruct 磁盘持久化协议格式
     * @return
     */
    public AppendMessageResult appendData(final DataStruct dataStruct);


    String getFileName();

    FileChannel getFileChannel();

    int getBlockSize();

    /**
     * 检查文件是否可用
     *
     * @return
     */
    boolean isAvailable();


    /**
     * 关闭文件
     */
    public abstract void close();

    /**
     * 清除文件的资源，但不真正删除文件
     *
     * @return
     */
    public abstract void clean();

    /**
     * 删除文件
     */
    public abstract void delete();


}
