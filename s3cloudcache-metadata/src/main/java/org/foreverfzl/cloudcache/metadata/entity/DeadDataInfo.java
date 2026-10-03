package org.foreverfzl.cloudcache.metadata.entity;

/**
 * 如果一个物理block上传三次都上传失败，则会包装成这个对象放入到死信队列中
 */
/**
 * 中文：记录逻辑块失败时的不可变定位；除上传耗尽重试外，分配/恢复失败也可产生此记录；不包含数据本体或文件引用。
 * English: Immutable coordinates of a failed logical block; allocation/recovery failures may also produce it, and it contains neither payload nor a file reference.
 */
public class DeadDataInfo {

    /**
     * 中文：失败所属实例标识。
     * English: Identifier of the owning instance.
     */
    private final String instanceName;
    /**
     * 中文：目标 S3 Bucket 名称。
     * English: Target S3 bucket name.
     */
    private final String bucketName;
    /**
     * 中文：保留 WAL 文件的逻辑起始字节偏移。
     * English: Logical starting byte offset of the retained WAL file.
     */
    private final long fileFromOffset;
    /**
     * 中文：失败块的文件内零基序号。
     * English: Zero-based index of the failed block within its file.
     */
    private final int logicalIndex;
    /**
     * 中文：失败块预期使用的对象键；不能据此认定远端对象不存在。
     * English: Intended object key; its presence here does not prove the remote object is absent.
     */
    private final String s3Key;

    /**
     * 中文：捕获失败身份供调用方读取保留的 WAL；构造不更改上传或文件状态。
     * English: Captures failure identity for reading retained WAL; construction changes neither upload nor file state.
     * @param instanceName 实例标识；English: Instance identifier.
     * @param bucketName Bucket 名称；English: Bucket name.
     * @param fileFromOffset WAL 逻辑起始字节偏移；English: Logical WAL start in bytes.
     * @param logicalIndex 文件内块序号；English: Within-file block index.
     * @param s3Key 预期对象键；English: Intended object key.
     */
    public DeadDataInfo(String instanceName, String bucketName, long fileFromOffset, int logicalIndex, String s3Key) {
        this.instanceName = instanceName;
        this.bucketName = bucketName;
        this.fileFromOffset = fileFromOffset;
        this.logicalIndex = logicalIndex;
        this.s3Key = s3Key;
    }

    /**
     * 中文：返回所属实例标识。
     * English: Returns the owning instance identifier.
     * @return 实例标识；English: Instance identifier.
     */
    public String getInstanceName() {
        return instanceName;
    }

    /**
     * 中文：返回目标 Bucket。
     * English: Returns the target bucket.
     * @return Bucket 名称；English: Bucket name.
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * 中文：返回保留 WAL 的逻辑基址。
     * English: Returns the retained WAL's logical base offset.
     * @return 文件逻辑起始字节偏移；English: Logical file start in bytes.
     */
    public long getFileFromOffset() {
        return fileFromOffset;
    }

    /**
     * 中文：返回失败块序号。
     * English: Returns the failed block index.
     * @return 文件内零基索引；English: Zero-based within-file index.
     */
    public int getLogicalIndex() {
        return logicalIndex;
    }

    /**
     * 中文：返回失败时捕获的对象键。
     * English: Returns the object key captured at failure.
     * @return 预期对象键；English: Intended object key.
     */
    public String getS3Key() {
        return s3Key;
    }
}
