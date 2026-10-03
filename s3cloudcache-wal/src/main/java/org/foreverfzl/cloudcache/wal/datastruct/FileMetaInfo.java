package org.foreverfzl.cloudcache.wal.datastruct;

/**
 * 每个文件元数据区域的格式，该类仅供参考
 */
/**
 * 中文：WAL文件头前三个long的快照：偏移0读位点、8上传位点、16更新时间。64起的逐块上传确认另由DefaultMappedFile管理，此对象不涵盖整个4KiB头。
 * English: Snapshot of the first three WAL-header longs: read position at 0, upload position at 8, and timestamp at 16. Per-block acknowledgements from offset 64 are managed separately; this object is not the full 4KiB header.
 */
public class FileMetaInfo {

    /**
     * 中文：文件头保留空间4096字节；所有数据区位点需加该值才是物理文件偏移。
     * English: 4096 reserved header bytes; add this to data-area positions to obtain physical file offsets.
     */
    public static final long FILE_META_SIZE = 4 * 1024;

    //元数据区域严格按照下面的结构
    /**
     * 中文：连续可恢复/已刷盘检查点，相对数据区的字节位点；恢复初始化也会合并上传确认位点。
     * English: Contiguous recovery/flush checkpoint in data-area bytes; recovery initialization also incorporates upload acknowledgement.
     */
    private final long readPosition;
    /**
     * 中文：连续上传确认水位线，单位为数据区字节；不能单独表达乱序成功块。
     * English: Contiguous upload watermark in data-area bytes; it cannot alone represent out-of-order completed blocks.
     */
    private final long uploadPosition;
    /**
     * 中文：检查点快照的毫秒时间戳，不作为事务序号使用。
     * English: Checkpoint timestamp in epoch milliseconds, not a transaction sequence.
     */
    private final long updateTime;

    /**
     * 中文：保存头部快照，不在构造器中校验范围或对齐。
     * English: Stores the header snapshot without constructor range or alignment validation.
     *
     * @param readPosition 中文：数据区读位点，字节；English: data-area read position in bytes
     * @param uploadPosition 中文：数据区连续上传位点，字节；English: contiguous upload position in bytes
     * @param updateTime 中文：Unix纪元毫秒；English: epoch milliseconds
     */
    public FileMetaInfo(long readPosition, long uploadPosition, long updateTime) {
        this.readPosition = readPosition;
        this.uploadPosition = uploadPosition;
        this.updateTime = updateTime;
    }

    /**
     * 中文：读取快照中的读位点。
     * English: Reads the snapshot's read position.
     *
     * @return 中文：数据区字节位点；English: data-area byte position
     */
    public long getReadPosition() {
        return readPosition;
    }

    /**
     * 中文：读取快照中的连续上传位点。
     * English: Reads the snapshot's contiguous upload position.
     *
     * @return 中文：数据区字节位点；English: data-area byte position
     */
    public long getUploadPosition() {
        return uploadPosition;
    }

    /**
     * 中文：读取快照更新时间。
     * English: Reads the checkpoint timestamp.
     *
     * @return 中文：Unix纪元毫秒；English: epoch milliseconds
     */
    public long getUpdateTime() {
        return updateTime;
    }
}
