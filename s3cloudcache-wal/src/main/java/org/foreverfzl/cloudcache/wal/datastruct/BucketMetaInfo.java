package org.foreverfzl.cloudcache.wal.datastruct;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * 一个Bucket对应的元数据 文件内容格式
 */
public class BucketMetaInfo {
    /** 4 KiB 元数据页扣除 24 字节固定头后，前缀允许占用的最大 UTF-8 字节数。 */
    public static final int MAX_PREFIX_BYTES = 4096 - 24;

    //是否需要数据恢复，1代表需要，0代表不需要
    private int isDirty;

    private int blockSize;

    private long fileSize;

    private int crc;

    private final int dataLen;

    /**
     * 用户自定义 S3 Key 前缀
     */
    private final byte[] data;


    public BucketMetaInfo(int blockSize,long fileSize,  String s3KeyPrefix) {
        this(1, blockSize, fileSize, prefixBytes(s3KeyPrefix));
    }

    public BucketMetaInfo(int isDirty, int blockSize, long fileSize, byte[] data) {
        if (isDirty != 0 && isDirty != 1) throw new IllegalArgumentException("isDirty must be 0 or 1");
        if (blockSize <= 0 || (blockSize & (blockSize - 1)) != 0
                || fileSize <= 0 || fileSize % blockSize != 0 || fileSize / blockSize > 1024) {
            throw new IllegalArgumentException("Invalid WAL block/file size in bucket metadata");
        }
        if (data == null || data.length > MAX_PREFIX_BYTES) {
            throw new IllegalArgumentException("S3 prefix must fit in " + MAX_PREFIX_BYTES + " UTF-8 bytes");
        }
        this.isDirty = isDirty;
        this.fileSize = fileSize;
        this.blockSize = blockSize;
        this.data = data.clone();
        this.dataLen = data.length;
        updateCrc();
    }

    private static byte[] prefixBytes(String prefix) {
        if (prefix == null) throw new IllegalArgumentException("s3KeyPrefix cannot be null");
        return prefix.getBytes(StandardCharsets.UTF_8);
    }


    /**
     * 更新 CRC
     */
    public void updateCrc() {
        CRC32 crc32 = new CRC32();
        // CRC32.update(int) 只处理低 8 位。V2 必须校验完整字段，fileSize 始终使用 long。
        byte[] header = ByteBuffer.allocate(Integer.BYTES + Long.BYTES + Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN).putInt(blockSize).putLong(fileSize).putInt(dataLen).array();
        crc32.update(header);
        crc32.update(data);
        this.crc = (int) crc32.getValue();
    }

    /** 仅用于校验旧文件，不再用于写新文件；保留旧算法只取每个数值低 8 位的行为。 */
    public int legacyCrc() {
        CRC32 crc32 = new CRC32();
        crc32.update(blockSize);
        crc32.update((int) fileSize);
        crc32.update(dataLen);
        crc32.update(data);
        return (int) crc32.getValue();
    }

    public int getCrc() {
        return crc;
    }

    public int getDataLen() {
        return dataLen;
    }

    public byte[] getData() {
        return data.clone();
    }

    public int getIsDirty() {
        return isDirty;
    }

    public long getFileSize() {
        return fileSize;
    }

    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public String toString() {
        return "BucketMetaInfo{" +
                "isDirty=" + isDirty +
                ", blockSize=" + blockSize +
                ", fileSize=" + fileSize +
                ", crc=" + crc +
                ", dataLen=" + dataLen +
                ", data=" + Arrays.toString(data) +
                '}';
    }
}
