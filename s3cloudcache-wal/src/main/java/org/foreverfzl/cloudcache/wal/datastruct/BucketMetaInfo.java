package org.foreverfzl.cloudcache.wal.datastruct;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * 一个Bucket对应的元数据 文件内容格式
 */
/**
 * 中文：Bucket恢复配置值对象：dirty、块大小、WAL数据区大小及UTF-8前缀。getCrc返回V2校验值；旧文件兼容校验必须显式使用legacyCrc。
 * English: Bucket recovery configuration: dirty flag, block size, WAL data-area size, and UTF-8 prefix. getCrc returns V2 CRC; legacy validation must explicitly use legacyCrc.
 */
public class BucketMetaInfo {
    /** 4 KiB 元数据页扣除 24 字节固定头后，前缀允许占用的最大 UTF-8 字节数。 */
    /**
     * 中文：4KiB页减24字节固定头，限制UTF-8字节数而非字符数。
     * English: 4KiB minus the 24-byte fixed header; limits UTF-8 bytes rather than characters.
     */
    public static final int MAX_PREFIX_BYTES = 4096 - 24;

    //是否需要数据恢复，1代表需要，0代表不需要
    /**
     * 中文：0/1恢复提示；文件存在性与有效性仍需另行检查。
     * English: Recovery hint 0/1; file existence and validity still require separate checks.
     */
    private int isDirty;

    /**
     * 中文：WAL逻辑块容量，字节，必须为正的2次幂。
     * English: Logical WAL block capacity in bytes, required to be a positive power of two.
     */
    private int blockSize;

    /**
     * 中文：单WAL数据区字节数，不含4096字节文件头。
     * English: WAL data-area bytes, excluding the 4096-byte file header.
     */
    private long fileSize;

    /**
     * 中文：V2配置CRC32的32位模式，不覆盖可变dirty标志。
     * English: 32-bit V2 configuration CRC; excludes the mutable dirty flag.
     */
    private int crc;

    /**
     * 中文：UTF-8前缀字节数，范围0..4072。
     * English: UTF-8 prefix byte count, from 0 through 4072.
     */
    private final int dataLen;

    /**
     * 用户自定义 S3 Key 前缀
     */
    /**
     * 中文：防御性复制的前缀内容；与计算CRC使用的内容一致。
     * English: Defensively copied prefix, kept consistent with the CRC input.
     */
    private final byte[] data;


    /**
     * 中文：创建默认dirty=1的配置，并在触碰文件前校验格式约束。
     * English: Creates configuration with dirty=1 and validates format constraints before file operations.
     *
     * @param blockSize 中文：逻辑块容量，字节；English: logical block bytes
     * @param fileSize 中文：WAL数据区容量，字节；English: WAL data-area bytes
     * @param s3KeyPrefix 中文：非null前缀，按UTF-8计长度；English: non-null prefix measured in UTF-8 bytes
     * @throws IllegalArgumentException 中文：配置不满足块布局或前缀约束；English: layout or prefix constraints are violated
     */
    public BucketMetaInfo(int blockSize,long fileSize,  String s3KeyPrefix) {
        this(1, blockSize, fileSize, prefixBytes(s3KeyPrefix));
    }

    /**
     * 中文：保存并复制已解码配置；校验块布局和最多1024块限制，计算完整字段V2 CRC，不自动识别来源文件版本。
     * English: Copies decoded configuration, validates layout and the 1024-block limit, and computes full-field V2 CRC; it does not infer the source file version.
     *
     * @param isDirty 中文：0或1；English: 0 or 1
     * @param blockSize 中文：块容量，字节；English: block bytes
     * @param fileSize 中文：数据区容量，字节；English: data-area bytes
     * @param data 中文：UTF-8前缀字节；English: UTF-8 prefix bytes
     */
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

    /**
     * 中文：检查null并按UTF-8编码；字节长度上限在委托构造器中验证。
     * English: Rejects null and encodes UTF-8; the delegated constructor checks byte-length limits.
     *
     * @param prefix 中文：S3 Key前缀；English: S3 key prefix
     * @return 中文：新UTF-8数组；English: new UTF-8 array
     */
    private static byte[] prefixBytes(String prefix) {
        if (prefix == null) throw new IllegalArgumentException("s3KeyPrefix cannot be null");
        return prefix.getBytes(StandardCharsets.UTF_8);
    }


    /**
     * 更新 CRC
     */
    /**
     * 中文：以固定大端4+8+4字节编码块大小、文件大小和前缀长度，再校验完整前缀；dirty与格式tag不计入此CRC。
     * English: Encodes block size, file size, and prefix length as fixed big-endian 4+8+4 bytes, then hashes the full prefix; dirty and the format tag are excluded.
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
    /**
     * 中文：精确兼容旧算法只取数值低8位的行为；long强转仅用于取低位，不表示支持旧程序创建2GiB文件。
     * English: Reproduces the legacy algorithm's low-byte-only numeric hashing. The long cast extracts low bits; it does not imply old software could create 2GiB files.
     *
     * @return 中文：旧格式CRC32位模式；English: legacy CRC32 bit pattern
     */
    public int legacyCrc() {
        CRC32 crc32 = new CRC32();
        crc32.update(blockSize);
        crc32.update((int) fileSize);
        crc32.update(dataLen);
        crc32.update(data);
        return (int) crc32.getValue();
    }

    /**
     * 中文：读取当前V2 CRC，包括从旧文件构造的值对象也返回V2值。
     * English: Reads the current V2 CRC, even for a value object decoded from a legacy file.
     *
     * @return 中文：V2 CRC32位模式；English: V2 CRC32 bit pattern
     */
    public int getCrc() {
        return crc;
    }

    /**
     * 中文：读取编码后前缀长度。
     * English: Reads encoded prefix length.
     *
     * @return 中文：UTF-8字节数；English: UTF-8 byte count
     */
    public int getDataLen() {
        return dataLen;
    }

    /**
     * 中文：返回前缀副本，避免调用者修改内部数据使CRC失效。
     * English: Returns a prefix copy to prevent caller mutation from invalidating the internal CRC.
     *
     * @return 中文：新分配的前缀字节数组；English: newly allocated prefix byte array
     */
    public byte[] getData() {
        return data.clone();
    }

    /**
     * 中文：读取恢复提示。
     * English: Reads the recovery hint.
     *
     * @return 中文：0或1；English: 0 or 1
     */
    public int getIsDirty() {
        return isDirty;
    }

    /**
     * 中文：读取WAL数据区容量。
     * English: Reads WAL data-area capacity.
     *
     * @return 中文：字节数，不含文件头；English: bytes excluding the file header
     */
    public long getFileSize() {
        return fileSize;
    }

    /**
     * 中文：读取逻辑块容量。
     * English: Reads logical block capacity.
     *
     * @return 中文：块字节数；English: block bytes
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * 中文：生成配置诊断文本，前缀按字节数组表示。
     * English: Produces configuration diagnostics with the prefix represented as bytes.
     *
     * @return 中文：配置诊断字符串；English: configuration diagnostic string
     */
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
