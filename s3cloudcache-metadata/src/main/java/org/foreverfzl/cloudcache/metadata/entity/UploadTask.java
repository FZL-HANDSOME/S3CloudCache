package org.foreverfzl.cloudcache.metadata.entity;

/**
 * 上传任务实体
 */
/**
 * 中文：只携带逻辑定位的上传候选任务，不持有物理 Block 或 WAL 引用；消费者必须再次检查身份和上传资格。
 * English: Upload candidate carrying logical coordinates only, without physical-block or WAL references; consumers must revalidate identity and eligibility.
 */
public class UploadTask {
    //文件名字
    /**
     * 中文：WAL 文件逻辑起始字节偏移，也用于文件名；不是该次记录的文件内偏移。
     * English: Logical WAL file start in bytes, also used in its filename; not the record's within-file offset.
     */
    private final long fileFromOffset;
    //block逻辑索引
    /**
     * 中文：文件内零基 Block 序号；项目 BlockKey 使用低 10 位编码该值。
     * English: Zero-based block index within the file, encoded in the low ten bits of BlockKey.
     */
    private final int logicalIndex;

    /**
     * 中文：保存定位信息，不执行上传、不取得资源租约。
     * English: Stores coordinates without uploading or acquiring resource leases.
     * @param fileName WAL 文件逻辑起始字节偏移，名称沿用旧接口；English: Logical WAL start in bytes, with a legacy parameter name.
     * @param logicalIndex 文件内逻辑块索引；English: Logical block index within the file.
     */
    public UploadTask(long fileName, int logicalIndex) {

        this.fileFromOffset = fileName;
        this.logicalIndex = logicalIndex;
    }



    /**
     * 中文：返回 WAL 文件基址。
     * English: Returns the WAL file base offset.
     * @return 文件逻辑起始字节偏移；English: Logical file start in bytes.
     */
    public long getFileFromOffset() {
        return fileFromOffset;
    }

    /**
     * 中文：返回文件内块索引。
     * English: Returns the within-file block index.
     * @return 零基逻辑块序号；English: Zero-based logical block index.
     */
    public int getLogicalIndex() {
        return logicalIndex;
    }
}
