package org.foreverfzl.cloudchache.common.cloudcahceEnum;

/**
 * WAL持久化文件大小枚举类
 * 中文：WAL 数据区的字节容量预设；实际文件还包含文件头元数据，不含在这些值内。
 * English: Byte-capacity presets for the WAL data region; the physical file also contains separate header metadata.
 */
public enum WalFileSize {
    /**
     * 中文：128 MiB 数据区。
     * English: A 128 MiB data region.
     */
    SIZE_128MB(128L * 1024 * 1024),
    /**
     * 中文：256 MiB 数据区。
     * English: A 256 MiB data region.
     */
    SIZE_256MB(256L * 1024 * 1024),      // 256MB
    /**
     * 中文：512 MiB 数据区。
     * English: A 512 MiB data region.
     */
    SIZE_512MB(512L * 1024 * 1024),      // 512MB
    /**
     * 中文：1 GiB 数据区，为 BucketConfig 默认值。
     * English: A 1 GiB data region, the BucketConfig default.
     */
    SIZE_1G(1024L * 1024 * 1024),       // 1GB
    /**
     * 中文：2 GiB 数据区，使用 long 保留完整容量。
     * English: A 2 GiB data region retained as a long value.
     */
    SIZE_2G(2L * 1024 * 1024 * 1024);   // 2GB

    /**
     * 中文：不含文件头的 WAL 数据区字节数。
     * English: WAL data-region bytes excluding the file header.
     */
    private final long bytes;

    /**
     * 中文：保存当前 WAL 数据区容量预设。
     * English: Stores the WAL data-region capacity preset.
     * @param bytes 中文：数据区字节容量；English: data-region capacity in bytes
     */
    WalFileSize(long bytes) {
        this.bytes = bytes;
    }

    /**
     * 中文：取得用于 BucketConfig.walFileSize 的数据区容量。
     * English: Returns the data-region capacity for BucketConfig.walFileSize.
     * @return 中文：不含文件头的字节数；English: bytes excluding the file header
     */
    public long getBytes() {
        return bytes;
    }
}
