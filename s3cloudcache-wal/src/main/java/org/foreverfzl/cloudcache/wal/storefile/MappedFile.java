package org.foreverfzl.cloudcache.wal.storefile;

import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.datastruct.WalDataStruct;

import java.nio.channels.FileChannel;

public interface MappedFile {

    public void init(boolean isWarm,boolean isLockMemory);

    /**
     * 文件预热，并且根据配置选择是否锁定预热PageCache
     *
     * @param pages
     */
    public void warm(int pages,boolean isLockMemory);

    /**
     * 读取该文件数据
     *
     * @param readOffset 起始位置
     * @param size       文件大小
     * @return
     */
    public WalDataStruct getData(final long readOffset, final long size);

    /**
     *
     * @param dataStruct 磁盘持久化协议格式
     * @return
     */
    public AppendMessageResult appendData(final DataStruct dataStruct);


    String getFileName();

    FileChannel getFileChannel();

    /**
     * 检查文件是否可用
     *
     * @return
     */
    boolean isAvailable();


    /**
     * 关闭文件
     */
    public void close();

    /**
     * 删除文件，将文件标记为删除
     */
    public void delete();

    /**
     * 、
     * 释放文件的引用
     *
     * @return
     */
    public int release();

    /**
     * 真正的清除该文件
     *
     * @return
     */
    public abstract void clean();

    /**
     * 获取该文件的引用
     *
     * @return
     */
    public int hold();


}
