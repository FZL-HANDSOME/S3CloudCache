package org.foreverfzl.cloudchache.common.config;

import org.foreverfzl.cloudchache.common.cloudcahceEnum.BlockSizeLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.BlockUploadConcurrencyLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.CacheSizeLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.WalFileSize;

import java.nio.charset.StandardCharsets;

/**
 * 中文：单个 Bucket 的可变配置输入；构造器和 setter 只赋值，实例启动通过 copyAndValidate 建立独立快照。
 * English: Mutable input for one Bucket; constructors and setters only assign values, while startup uses copyAndValidate for an independent snapshot.
 * 中文：配置不负责分配资源；不要在快照复制过程中并发修改它，已启动实例不跟随调用方对象热更新。
 * English: Configuration does not allocate resources; do not mutate it during snapshot creation, and running instances do not track caller-side changes.
 */
public class BucketConfig {
    // bucketMeta 固定 4KB，前 24 字节保存状态、尺寸、校验和及前缀长度。
    /**
     * 中文：可持久化的前缀 UTF-8 字节上限，等于 4096 字节元数据减去 24 字节固定头。
     * English: Persistable UTF-8 prefix limit: 4096 metadata bytes minus the 24-byte fixed header.
     */
    private static final int MAX_PREFIX_BYTES = 4096 - 24;
    /**
     * 生成S3key的用户自定义前缀
     * 中文：默认未设置；快照时必须非 null，允许空串，UTF-8 长度至多 4072 字节；参与恢复配置匹配。
     * English: Initially unset; snapshots require non-null, allow empty text, and limit UTF-8 length to 4072 bytes; participates in recovery configuration matching.
     */
    public String s3KeyPrefix;

    /**
     * WAL 文件大小，默认 1G
     * 中文：数据区容量，默认 1 GiB，不含额外 4096 字节文件头；必须为 blockSize 的正整数倍且至多 1024 块。
     * English: Data-region capacity, default 1 GiB, excluding the extra 4096-byte file header; must contain 1 to 1024 complete blocks.
     */
    public Long walFileSize = WalFileSize.SIZE_1G.getBytes();


    /**
     * 缓冲区大小
     * 中文：单 Bucket 分配的堆外缓存字节数，默认 256 MiB；至少一块，整块数量不能超过 int 上限，允许余数。
     * English: Off-heap bytes allocated per Bucket, default 256 MiB; at least one block, an int-sized whole-block count, with a remainder allowed.
     */
    public Long cacheSize = CacheSizeLevel.TINE.getBytes();

    /**
     * Block的大小，默认为4MB
     * 中文：逻辑 WAL 块与物理缓存块的容量，默认 4 MiB；快照要求至少 4 字节且为 2 的幂。
     * English: Capacity shared by logical WAL and physical cache blocks, default 4 MiB; snapshots require a power of two of at least 4 bytes.
     * 中文：WAL 容量还需容纳每条记录的 12 字节协议头及对齐；S3 对象只拼接 Value，故单条 Value 不能直接占满 WAL 块。
     * English: WAL capacity includes each record's 12-byte header and alignment, whereas S3 contains concatenated Values; one Value cannot fill the entire WAL block.
     */
    public Integer blockSize = BlockSizeLevel.SMALL.getBytes();

    /**
     * Block并发上传大小，默认为8个
     * 中文：单 Bucket 的同时上传许可数，默认 8，必须为正；不表示预先建立同等数量的平台线程。
     * English: Per-Bucket concurrent upload permits, default 8 and strictly positive; not a preallocated platform-thread count.
     */
    public Integer blockUpLoadCount = BlockUploadConcurrencyLevel.NORMAL.getConcurrency();


    /**
     * 是否预热WAL文件
     * 中文：默认 true；控制新 WAL 映射预热，快照不接受 null；预热不是上传确认，也不代替持久化协议。
     * English: Default true; controls warming new WAL mappings and must be non-null; warming is neither upload acknowledgement nor a durability protocol.
     */
    public Boolean isWarmWalFile = true;

    /**
     * 是否锁定持久化文件对应的操作系统PageCache缓冲区
     * 中文：默认 false；true 请求平台内存锁定，受 OS 权限和额度限制，失败只记录警告；快照不接受 null。
     * English: Default false; true requests OS memory locking subject to permissions and limits, with warnings on failure; snapshots reject null.
     */
    public Boolean isLockMappedFilePageCache = false;

    /**
     * 缓存上传后是否进行一次headObject校验，如果为true会增加一次网络请求。
     * 中文：默认 false；true 时上传器追加 HEAD 检查；快照要求非 null，该开关不改变 Value 的存储布局。
     * English: Default false; true adds uploader HEAD verification; snapshots require non-null, and the flag does not alter Value layout.
     */
    public Boolean enableHeadCheck = false;

    /**
     * 将文件的元数据写入到文件开头，数据恢复是方便，默认为5s，时间长了刷新慢，如果宕机恢复数据可能变多，如果时间太短了性能会下降
     * 中文：后台文件元数据刷写循环的间隔，单位毫秒，默认 5000，必须为正；不是每条写入的强制等待时间。
     * English: Background file-metadata flush-loop interval in milliseconds, default 5000 and strictly positive; not a mandatory delay per write.
     */
    public Integer flushFileMetaInfoTime = 5000;

    /**
     * 检查文件，并删除可以删除的文件，默认10s
     * 中文：后台 WAL 清理检查间隔，单位毫秒，默认 10000，必须为正；到期只检查，不绕过安全删除条件。
     * English: Background WAL-cleanup check interval in milliseconds, default 10000 and strictly positive; expiry checks eligibility without bypassing deletion safety.
     */
    public Integer chackMappedFileTime =10000;


    /**
     * 中文：创建默认容量和调度配置；前缀仍为 null，调用方必须在校验前提供前缀。
     * English: Creates default capacities and scheduling settings; the prefix remains null and must be supplied before validation.
     */
    public BucketConfig() {

    }

    /**
     * 创建运行时配置副本，并在申请堆外内存、创建或覆盖 WAL 元数据之前校验必要约束。
     * 副本不与调用方共享可变字段；cacheSize 的不足一块余数允许保留，不影响记录完整性。
     * 中文：复制所有标量配置后才校验副本，不检查实际剩余内存或磁盘空间，也不创建资源。
     * English: Copies every scalar setting before validating the copy; it neither checks available memory/disk nor creates resources.
     * @return 中文：通过必要约束检查的独立可变配置对象；English: independent mutable configuration satisfying required invariants
     * @throws IllegalArgumentException 中文：任一配置违反尺寸、范围或非空约束；English: a setting violates size, range, or non-null constraints
     */
    public BucketConfig copyAndValidate() {
        BucketConfig copy = new BucketConfig();
        copy.s3KeyPrefix = this.s3KeyPrefix;
        copy.walFileSize = this.walFileSize;
        copy.cacheSize = this.cacheSize;
        copy.blockSize = this.blockSize;
        copy.blockUpLoadCount = this.blockUpLoadCount;
        copy.isWarmWalFile = this.isWarmWalFile;
        copy.isLockMappedFilePageCache = this.isLockMappedFilePageCache;
        copy.enableHeadCheck = this.enableHeadCheck;
        copy.flushFileMetaInfoTime = this.flushFileMetaInfoTime;
        copy.chackMappedFileTime = this.chackMappedFileTime;
        copy.validate();
        return copy;
    }

    /**
     * 中文：检查位移除法、10-bit 块索引、物理池计数和元数据布局所依赖的配置前提。
     * English: Checks assumptions required by shift division, 10-bit block indices, physical-pool counts, and metadata layout.
     * @throws IllegalArgumentException 中文：发现首个非法配置；English: the first invalid setting is encountered
     */
    private void validate() {
        // 中文：先验证 blockSize，使后续取模和除法只处理非零的有效块容量。
        // English: Validate blockSize first so later modulo and division use a valid nonzero capacity.
        require(blockSize != null && blockSize >= 4 && (blockSize & (blockSize - 1)) == 0,
                "blockSize must be a power of two and at least 4 bytes");
        require(walFileSize != null && walFileSize > 0 && walFileSize % blockSize == 0
                        && walFileSize / blockSize <= 1024,
                "walFileSize must contain between 1 and 1024 complete blocks");
        // 中文：只约束实际物理块数量，不拒绝分配区尾部不足一块的余量。
        // English: Constrain the physical block count without rejecting an unused sub-block allocation tail.
        require(cacheSize != null && cacheSize >= blockSize && cacheSize / blockSize <= Integer.MAX_VALUE,
                "cacheSize must contain between 1 and Integer.MAX_VALUE physical blocks");
        require(blockUpLoadCount != null && blockUpLoadCount > 0, "blockUpLoadCount must be positive");
        require(flushFileMetaInfoTime != null && flushFileMetaInfoTime > 0,
                "flushFileMetaInfoTime must be positive");
        require(chackMappedFileTime != null && chackMappedFileTime > 0, "chackMappedFileTime must be positive");
        require(isWarmWalFile != null, "isWarmWalFile must not be null");
        require(isLockMappedFilePageCache != null, "isLockMappedFilePageCache must not be null");
        require(enableHeadCheck != null, "enableHeadCheck must not be null");
        // 中文：磁盘元数据存储的是 UTF-8 字节，不能用 Java 字符个数替代容量检查。
        // English: Metadata stores UTF-8 bytes, so Java character counts cannot replace this capacity check.
        require(s3KeyPrefix != null && s3KeyPrefix.getBytes(StandardCharsets.UTF_8).length <= MAX_PREFIX_BYTES,
                "s3KeyPrefix must not be null or exceed " + MAX_PREFIX_BYTES + " UTF-8 bytes");
    }

    /**
     * 中文：为配置前置条件提供一致的失败类型，不修改配置或申请资源。
     * English: Provides a consistent failure type for configuration preconditions without mutation or resource allocation.
     * @param valid 中文：待检查的条件；English: condition to check
     * @param message 中文：失败时的诊断信息；English: diagnostic message on failure
     * @throws IllegalArgumentException 中文：条件为 false；English: the condition is false
     */
    private static void require(boolean valid, String message) {
        if (!valid) throw new IllegalArgumentException(message);
    }

    /**
     * 中文：保存显式容量和内存选项，HEAD 与调度间隔继续采用字段默认值；本构造器不提前校验。
     * English: Stores explicit capacities and memory options while retaining default HEAD and scheduling settings; this constructor does not validate.
     * @param s3KeyPrefix 中文：对象键前缀；English: object-key prefix
     * @param walFileSize 中文：WAL 数据区字节数，不含文件头；English: WAL data-region bytes excluding the file header
     * @param cacheSize 中文：Bucket 堆外池总字节数；English: total bytes in the Bucket off-heap pool
     * @param blockSize 中文：单块字节容量；English: byte capacity per block
     * @param blockUpLoadCount 中文：上传并发许可上限；English: concurrent upload permit limit
     * @param isWarmWalFile 中文：是否预热新映射；English: whether to warm new mappings
     * @param isLockMappedFilePageCache 中文：是否请求 OS 锁页；English: whether to request OS page locking
     */
    public BucketConfig(String s3KeyPrefix, Long walFileSize, Long cacheSize, Integer blockSize, Integer blockUpLoadCount,
                        Boolean isWarmWalFile, Boolean isLockMappedFilePageCache) {
        this.s3KeyPrefix = s3KeyPrefix;
        this.walFileSize = walFileSize;
        this.cacheSize = cacheSize;
        this.blockSize = blockSize;
        this.blockUpLoadCount = blockUpLoadCount;
        this.isWarmWalFile = isWarmWalFile;
        this.isLockMappedFilePageCache = isLockMappedFilePageCache;
    }

    /**
     * 中文：更新待快照的 Key 前缀，UTF-8 长度约束在 copyAndValidate 时检查。
     * English: Updates the pending key prefix; copyAndValidate checks its UTF-8 byte limit.
     * @param s3KeyPrefix 中文：对象键前缀；English: object-key prefix
     * @return 中文：当前配置对象，便于链式设置；English: this configuration for chaining
     */
    public BucketConfig setS3KeyPrefix(String s3KeyPrefix) {
        this.s3KeyPrefix = s3KeyPrefix;
        return this;
    }

    /**
     * 中文：设置 WAL 数据区容量，不立即扩容已存在的文件。
     * English: Sets WAL data-region capacity without resizing existing files immediately.
     * @param walFileSize 中文：不含文件头的字节数，快照时验证完整块约束；English: bytes excluding the header, validated for whole blocks at snapshot time
     * @return 中文：当前配置对象；English: this configuration
     */
    public BucketConfig setWalFileSize(Long walFileSize) {
        this.walFileSize = walFileSize;
        return this;
    }

    /**
     * 中文：设置物理缓存池容量，只修改配置，不执行内存分配。
     * English: Sets physical cache-pool capacity without allocating memory.
     * @param cacheSize 中文：堆外池总字节数；English: total off-heap pool bytes
     * @return 中文：当前配置对象；English: this configuration
     */
    public BucketConfig setCacheSize(Long cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    /**
     * 中文：设置单块容量；快照时检查 2 的幂及其与 WAL 文件大小的关系。
     * English: Sets block capacity; snapshots check its power-of-two requirement and relationship to WAL file size.
     * @param blockSize 中文：单块字节数；English: bytes per block
     * @return 中文：当前配置对象；English: this configuration
     */
    public BucketConfig setBlockSize(Integer blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    /**
     * 中文：设置 Bucket 的上传并发上限，不调整已启动实例的许可数。
     * English: Sets the Bucket upload limit without changing permits in a running instance.
     * @param blockUpLoadCount 中文：快照时必须为正的许可数量；English: permit count required to be positive at snapshot time
     * @return 中文：当前配置对象；English: this configuration
     */
    public BucketConfig setBlockUpLoadCount(Integer blockUpLoadCount) {
        this.blockUpLoadCount = blockUpLoadCount;
        return this;
    }

    /**
     * 中文：设置新 WAL 映射的预热选项；不立即触碰现有映射页。
     * English: Sets warming for new WAL mappings without touching existing mapped pages immediately.
     * @param warmWalFile 中文：是否预热，快照时不得为 null；English: whether to warm, non-null at snapshot time
     * @return 中文：当前配置对象；English: this configuration
     */
    public BucketConfig setWarmWalFile(Boolean warmWalFile) {
        this.isWarmWalFile = warmWalFile;
        return this;
    }

    /**
     * 中文：设置 OS 锁页请求选项，不保证操作系统一定接受请求。
     * English: Sets the OS page-lock request option without guaranteeing OS acceptance.
     * @param lockMappedFilePageCache 中文：是否请求锁页，快照时不得为 null；English: whether to request page locking, non-null at snapshot time
     * @return 中文：当前配置对象；English: this configuration
     */
    public BucketConfig setLockMappedFilePageCache(Boolean lockMappedFilePageCache) {
        this.isLockMappedFilePageCache = lockMappedFilePageCache;
        return this;
    }

    /**
     * 中文：设置上传后附加 HEAD 检查的选项，不在此发起请求。
     * English: Sets the post-upload HEAD-check option without making a request here.
     * @param enableHeadCheck 中文：是否进行 HEAD 检查，快照时不得为 null；English: whether to perform HEAD verification, non-null at snapshot time
     * @return 中文：当前配置对象；English: this configuration
     */
    public BucketConfig setEnableHeadCheck(Boolean enableHeadCheck) {
        this.enableHeadCheck = enableHeadCheck;
        return this;
    }

    /**
     * 中文：设置后台元数据刷新间隔，不等于对本次写入做同步刷盘。
     * English: Sets the background metadata flush interval rather than synchronously flushing the current write.
     * @param flushFileMetaInfoTime 中文：正数毫秒间隔，在快照时检查；English: positive millisecond interval checked at snapshot time
     * @return 中文：当前配置对象；English: this configuration
     */
    public BucketConfig setFlushFileMetaInfoTime(Integer flushFileMetaInfoTime) {
        this.flushFileMetaInfoTime = flushFileMetaInfoTime;
        return this;
    }

}
