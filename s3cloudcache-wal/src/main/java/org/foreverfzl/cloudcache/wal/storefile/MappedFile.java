package org.foreverfzl.cloudcache.wal.storefile;

import org.foreverfzl.cloudchache.common.disk.WalDataStruct;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface MappedFile {

    public void init(String fileName,long fileSize);

    /**
     * 文件预热，并且根据配置选择是否锁定预热PageCache
     *
     * @param pages
     */
    public void warm(int pages);

    String getFileName();

    FileChannel getFileChannel();

    /**
     * 检查文件是否可用
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

    /**、
     * 释放文件的引用
     * @return
     */
    public int release();

    /**
     * 真正的清除该文件
     * @return
     */
    public abstract boolean clean();

    /**
     * 获取该文件的引用
     * @return
     */
    public int hold();

    /**
     * 读取该文件指定的数据到ByteBuffer中
     * @param offset 起始位置
     * @param size 文件大小
     * @param byteBuffer 数据读取到这个缓冲区
     * @return
     */
    public boolean getData(final long offset, final int size, final ByteBuffer byteBuffer);

    /**
     *
     * @param offset 起始位置
     * @param size 文件大小
     * @param walDataStruct 磁盘持久化协议格式
     * @return
     */
    public boolean appendData(final long offset, final int size, final WalDataStruct walDataStruct);


}
