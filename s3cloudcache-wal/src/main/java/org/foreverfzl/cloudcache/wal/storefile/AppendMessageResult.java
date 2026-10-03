package org.foreverfzl.cloudcache.wal.storefile;

/**
 * 数据追加写入操作的结果封装类。
 * 包含写入状态、写入偏移量、写入字节数等信息。
 */
/**
 * 中文：WAL复制阶段的结果及逻辑定位。PUT_OK仅表示写到映射/PageCache，不是force或S3提交完成；本类不包含业务Value长度字段。
 * English: Result and logical location of the WAL-copy stage. PUT_OK means copied to the mapping/page cache, not force or S3 commit completion; this class has no business-value-length field.
 */
public class AppendMessageResult {

    /**
     * 写入状态
     */
    /**
     * 中文：本次WAL尝试状态，创建后不变。
     * English: Immutable status of this WAL attempt.
     */
    private final AppendStatus status;

    /**
     * 写入的文件引用
     */
    /**
     * 中文：借用目标文件引用，本结果对象不会自动hold或release。
     * English: Borrowed destination-file reference; this result does not hold or release it.
     */
    private final DefaultMappedFile defaultMappedFile;

    /**
     * 写入的目标文件名
     */
    /**
     * 中文：文件身份编号，也是跨文件逻辑起点；不同于文件内部的数据偏移。
     * English: File identity/logical start across files, not an offset within its data area.
     */
    private final long fileFromOffset;

    /**
     * 该数据在改文件的哪逻辑Block中
     */
    /**
     * 中文：记录所在文件内的逻辑块序号；-1表示尚未成功定位。
     * English: Logical block index within the file; -1 means no location has been assigned.
     */
    private int logicalIndex = -1;

    /**
     * block内部的offset
     */
    /**
     * 中文：记录头在逻辑WAL块内的字节偏移，不是S3对象Value偏移。
     * English: Byte offset of the record header within its WAL block, not a value offset in an S3 object.
     */
    private long blockOffset;

    /**
     * 写入时的时间戳
     */
    /**
     * 中文：结果创建时的Unix纪元毫秒，不是持久化完成时间。
     * English: Result-creation time in epoch milliseconds, not a durability-completion timestamp.
     */
    private final long storeTimestamp;


    /**
     * 中文：保存阶段结果；逻辑块及块内偏移可在预留完成后补充。
     * English: Stores the stage result; block identity and block-relative offset can be populated after reservation.
     *
     * @param defaultMappedFile 中文：借用目标文件；English: borrowed destination file
     * @param status 中文：本阶段状态；English: stage status
     * @param storeTimestamp 中文：Unix纪元毫秒；English: epoch milliseconds
     * @param fileFromOffset 中文：文件身份编号；English: file identity
     */
    public AppendMessageResult(DefaultMappedFile defaultMappedFile, AppendStatus status, long storeTimestamp, long fileFromOffset) {
        this.defaultMappedFile = defaultMappedFile;
        this.status = status;
        this.storeTimestamp = storeTimestamp;
        this.fileFromOffset = fileFromOffset;
    }


    /**
     * 创建一个表示失败的结果（不携带偏移和字节数信息）
     */
    /**
     * 中文：创建非成功结果；如果失败发生在空间预留之后，调用者仍可补充logicalIndex和blockOffset。
     * English: Creates a non-success result; callers may still set logicalIndex and blockOffset when failure follows reservation.
     *
     * @param defaultMappedFile 中文：对应文件；English: associated file
     * @param status 中文：失败或轮转原因；English: failure or rotation reason
     * @param fileFromOffset 中文：文件身份编号；English: file identity
     * @return 中文：带当前毫秒时间戳的结果；English: result timestamped with current epoch milliseconds
     */
    public static AppendMessageResult fail(DefaultMappedFile defaultMappedFile, AppendStatus status, long fileFromOffset) {
        return new AppendMessageResult(defaultMappedFile, status, System.currentTimeMillis(), fileFromOffset);
    }


    /**
     * 追加写入的状态枚举
     */
    /**
     * 中文：WAL追加阶段状态；只有END_OF_FILE和FILE_CLOSED由Manager自动轮转重试。
     * English: WAL-append statuses; the manager automatically rotates and retries only END_OF_FILE and FILE_CLOSED.
     */
    public enum AppendStatus {
        /**
         * 写入成功，继续后续业务或返回 ACK。
         */
        /**
         * 中文：更正历史ACK描述：仅确认WAL复制完成，用户最终成功必须由S3上传确认决定。
         * English: Corrects the historical ACK wording: only WAL copying is complete; final user success requires S3 acknowledgement.
         */
        PUT_OK,
        /**
         * 文件剩余空间不足，已写满
         */
        /**
         * 中文：当前文件空间耗尽，需要轮转后重试原记录。
         * English: Current file has no space; retry the original record after rotation.
         */
        END_OF_FILE,
        /**
         * 参数异常（如 walDataStruct 为空）
         */
        /**
         * 中文：记录参数被拒绝，此结果不表示已预留空间。
         * English: Record arguments were rejected; this result does not imply a reservation.
         */
        INVALID_ARGUMENT,
        /**
         * 写入过程中发生未知错误
         */
        /**
         * 中文：预留后复制异常；已预留位置不会回退。
         * English: Copying failed after reservation; the reserved position is not rolled back.
         */
        WRITER_FAILED,
        /**
         * 文件关闭
         */
        /**
         * 中文：写入准入已关闭，可由Manager选择下一个文件。
         * English: Write admission is closed; the manager may select the next file.
         */
        FILE_CLOSED,
        /**
         * 单条消息大小超过一个逻辑 Block，无法写入
         */
        /**
         * 中文：完整序列化记录大于单块容量，轮转也无法解决。
         * English: The serialized record exceeds one block; rotation cannot make it fit.
         */
        MESSAGE_TOO_LARGE,
    }

    /**
     * 判断写入是否成功
     */
    /**
     * 中文：只判断状态是否为PUT_OK，不检查持久化或远端状态。
     * English: Tests only PUT_OK, without checking local durability or remote status.
     *
     * @return 中文：WAL复制阶段是否成功；English: whether the WAL-copy stage succeeded
     */
    public boolean isOk() {
        return this.status == AppendStatus.PUT_OK;
    }

    /**
     * 中文：读取阶段状态。
     * English: Reads the stage status.
     *
     * @return 中文：WAL追加状态；English: WAL-append status
     */
    public AppendStatus getStatus() {
        return status;
    }

    /**
     * 中文：读取创建时间。
     * English: Reads result creation time.
     *
     * @return 中文：Unix纪元毫秒；English: epoch milliseconds
     */
    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    /**
     * 中文：读取文件身份编号。
     * English: Reads the file identity.
     *
     * @return 中文：文件逻辑起点；English: logical file start
     */
    public long getFileFromOffset() {
        return fileFromOffset;
    }

    /**
     * 中文：读取逻辑块定位。
     * English: Reads the logical block location.
     *
     * @return 中文：块序号，未定位时为-1；English: block index, or -1 if unassigned
     */
    public int getLogicalIndex() {
        return logicalIndex;
    }

    /**
     * 中文：补充预留所得块序号，不做边界校验。
     * English: Sets the reserved block index without bounds validation.
     *
     * @param logicalIndex 中文：文件内逻辑块序号；English: logical block index within the file
     */
    public void setLogicalIndex(int logicalIndex) {
        this.logicalIndex = logicalIndex;
    }

    /**
     * 中文：补充记录头的块内偏移。
     * English: Sets the block-relative record-header offset.
     *
     * @param blockOffset 中文：WAL块内字节偏移；English: byte offset within the WAL block
     */
    public void setBlockOffset(long blockOffset) {
        this.blockOffset = blockOffset;
    }

    /**
     * 中文：读取WAL记录头偏移。
     * English: Reads the WAL record-header offset.
     *
     * @return 中文：块内字节偏移；English: block-relative byte offset
     */
    public long getBlockOffset() {
        return blockOffset;
    }

    /**
     * 中文：返回借用的目标文件对象。
     * English: Returns the borrowed destination file.
     *
     * @return 中文：目标文件，不自动获取引用；English: destination file without acquiring a reference
     */
    public DefaultMappedFile getDefaultMappedFile() {
        return defaultMappedFile;
    }

    /**
     * 中文：生成状态诊断文本，不包含原始业务数据。
     * English: Produces diagnostic status text without original business data.
     *
     * @return 中文：诊断字符串；English: diagnostic string
     */
    @Override
    public String toString() {
        return "AppendMessageResult{" +
                "status=" + status +
                ", fileFromOffset=" + fileFromOffset +
                ", logicalIndex=" + logicalIndex +
                ", storeTimestamp=" + storeTimestamp +
                '}';
    }
}
