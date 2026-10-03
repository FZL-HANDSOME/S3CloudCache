package org.foreverfzl.cloudcache.wal.storefile;

import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;

import java.nio.channels.FileChannel;

/**
 * 中文：WAL映射文件的操作契约。追加到PageCache、强制刷盘、关闭准入、卸载映射与删除路径是不同阶段。
 * English: Contract for a WAL mapping. Page-cache append, durable forcing, admission closure, unmapping, and path deletion are separate stages.
 */
public interface MappedFile {

    /**
     * 文件预热，并且根据配置选择是否锁定预热PageCache
     *
     * @param pages
     */
    /**
     * 中文：预触碰映射页并按实现要求刷盘；锁页是否成功取决于平台。禁止对已有有效数据使用写零型预热。
     * English: Touches mapped pages and forces them as implemented; page locking is platform-dependent. Zero-writing warmup must not be used on existing data.
     *
     * @param pages 中文：两次 force 之间的页数；English: pages between force operations
     * @param isLockMemory 中文：是否请求锁页，不是成功保证；English: whether to request page locking, not a success guarantee
     */
    public void warm(int pages, boolean isLockMemory);

    /**
     *
     * @param dataStruct 磁盘持久化协议格式
     * @return
     */
    /**
     * 中文：尝试追加一条序列化记录；成功表示本阶段复制完成，不代表远端提交或 force 完成。
     * English: Attempts to append one serialized record; success means this copy stage completed, not remote commit or durable forcing.
     *
     * @param dataStruct 中文：提供记录头及Value的序列化对象；English: serializer supplying the header and value
     * @return 中文：状态和WAL定位信息，不是最终用户ACK；English: status and WAL location, not the final user acknowledgement
     */
    public AppendMessageResult appendData(final DataStruct dataStruct);


    /**
     * 中文：返回文件名，不是完整路径。
     * English: Returns the filename, not its full path.
     *
     * @return 中文：映射文件名；English: mapped filename
     */
    String getFileName();

    /**
     * 中文：返回借用的底层通道；调用方不得擅自关闭。
     * English: Returns a borrowed channel that callers must not close independently.
     *
     * @return 中文：底层文件通道，清理后可能为空；English: underlying channel, possibly null after cleanup
     */
    FileChannel getFileChannel();

    /**
     * 中文：返回单个逻辑WAL块容量，包含记录头与对齐占用。
     * English: Returns logical WAL block capacity, including record headers and alignment.
     *
     * @return 中文：块容量，字节；English: block capacity in bytes
     */
    int getBlockSize();

    /**
     * 检查文件是否可用
     *
     * @return
     */
    /**
     * 中文：查询是否接纳新写入；不能替代引用获取。
     * English: Checks write admission; it does not replace acquiring a reference.
     *
     * @return 中文：写入准入状态；English: write-admission state
     */
    boolean isAvailable();


    /**
     * 关闭文件
     */
    /**
     * 中文：关闭新写入准入，具体资源释放需要另外调用clean。
     * English: Closes admission for new writes; resource release requires clean.
     */
    public abstract void close();

    /**
     * 清除文件的资源，但不真正删除文件
     *
     * @return
     */
    /**
     * 中文：释放映射和通道但不删除文件；调用者需先结束所有资源使用。
     * English: Releases mappings and channels without deleting the file; callers must first finish all resource use.
     */
    public abstract void clean();

    /**
     * 删除文件
     */
    /**
     * 中文：删除磁盘路径；调用者必须先证明数据已安全确认并释放映射。
     * English: Deletes the disk path; callers must first establish safe acknowledgement and release the mapping.
     */
    public abstract void delete();


}
