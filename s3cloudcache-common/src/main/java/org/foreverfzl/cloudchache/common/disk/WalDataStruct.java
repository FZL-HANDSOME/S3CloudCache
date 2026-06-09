package org.foreverfzl.cloudchache.common.disk;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * 磁盘持久化协议格式
 */
public class WalDataStruct {
    //+----------------+---------------+--------------+------------------+-----------------+-------------------+
    //| Magic (8B)     | Key Len (4B)  | Value Len(4B)| Checksum (8B)    |   Key Bytes     |    Value Bytes    |
    //+----------------+---------------+--------------+------------------+-----------------+-------------------+
    //     0x53334343    变长(如 9B)      变长(如 500B)      CRC32 校验码        "ORDER_123"        [原始数据]

    // 固定魔数 0x53334343 (ASCII "S3CC")
    public static final int MAGIC_NUMBER = 0x53334343;

    // 协议头部长度固定为 16 字节 (8 + 4 + 4 + 8)
    public static final long HEADER_LENGTH = 24;

    private final long magic;
    private final int keyLen;
    private final int valueLen;
    private final long checksum;
    private final byte[] keyBytes;
    private final byte[] valueBytes;

    /**
     * 构造一个新的 WalDataStruct，自动计算字段长度及 CRC32 校验和。
     *
     * @param keyBytes   业务 Key 的字节数组
     * @param valueBytes 业务 Value 的字节数组
     */
    public WalDataStruct(byte[] keyBytes, byte[] valueBytes) {
        if (keyBytes == null) {
            throw new IllegalArgumentException("Key bytes cannot be null");
        }
        if (valueBytes == null) {
            throw new IllegalArgumentException("Value bytes cannot be null");
        }
        this.magic = MAGIC_NUMBER;
        this.keyLen = keyBytes.length;
        this.valueLen = valueBytes.length;
        this.keyBytes = keyBytes;
        this.valueBytes = valueBytes;
        this.checksum = (int) calculateCRC32(keyBytes, valueBytes);
    }

    /**
     * 基于 String 类型 Key 构造新的 WalDataStruct，自动计算长度及 CRC32 校验和。
     *
     * @param key        业务 Key 字符串
     * @param valueBytes 业务 Value 的字节数组
     */
    public WalDataStruct(String key, byte[] valueBytes) {
        this(key != null ? key.getBytes(StandardCharsets.UTF_8) : null, valueBytes);
    }

    /**
     * 用于反序列化时还原 WalDataStruct 的完整构造方法。
     */
    public WalDataStruct(long magic, int keyLen, int valueLen, long checksum, byte[] keyBytes, byte[] valueBytes) {
        this.magic = magic;
        this.keyLen = keyLen;
        this.valueLen = valueLen;
        this.checksum = checksum;
        this.keyBytes = keyBytes;
        this.valueBytes = valueBytes;
    }

    public long getMagic() {
        return magic;
    }

    public int getKeyLen() {
        return keyLen;
    }

    public int getValueLen() {
        return valueLen;
    }

    public long getChecksum() {
        return checksum;
    }

    public byte[] getKeyBytes() {
        return keyBytes;
    }

    public byte[] getValueBytes() {
        return valueBytes;
    }

    public String getKeyAsString() {
        return new String(keyBytes, StandardCharsets.UTF_8);
    }

    /**
     * 获取当前数据包序列化后的总字节数 (Header + Key + Value)
     */
    public long getSerializedSize() {
        long size = HEADER_LENGTH + keyLen + valueLen;
        //保证8字节对齐，返回8的整数
        return (size + 7) & ~7;
    }

    /**
     * 计算 Key 和 Value 联合的 CRC32 校验和。
     */
    private static long calculateCRC32(byte[] keyBytes, byte[] valueBytes) {
        CRC32 crc = new CRC32();
        if (keyBytes != null && keyBytes.length > 0) {
            crc.update(keyBytes);
        }
        if (valueBytes != null && valueBytes.length > 0) {
            crc.update(valueBytes);
        }
        return crc.getValue();
    }

    /**
     * 校验当前数据的校验和是否正确。
     */
    private boolean validateChecksum() {
        long computed = calculateCRC32(this.keyBytes, this.valueBytes);
        return this.checksum == computed;
    }

    /**
     * 校验魔数是否匹配。
     */
    public boolean validateMagic() {
        return this.magic == MAGIC_NUMBER;
    }


    /**
     * 序列化写入传入的 ByteBuffer。
     */
    public void serialize(ByteBuffer buffer) {
        buffer.putLong(magic);
        buffer.putInt(keyLen);
        buffer.putInt(valueLen);
        buffer.putLong(checksum);
        buffer.put(keyBytes);
        buffer.put(valueBytes);
    }

    /**
     * 从 ByteBuffer 中反序列化出 WalDataStruct 对象。
     * 请确保在调用前 buffer 中有足够的剩余空间。
     */
    public static WalDataStruct deserialize(ByteBuffer buffer) {
        if (buffer.remaining() < HEADER_LENGTH) {
            throw new IllegalArgumentException("Buffer remaining data is less than header length");
        }

        long magic = buffer.getLong();
        if (magic != MAGIC_NUMBER) {
            throw new IllegalArgumentException(String.format("Invalid magic number: 0x%08X", magic));
        }

        int keyLen = buffer.getInt();
        int valueLen = buffer.getInt();
        long checksum = buffer.getLong();

        if (buffer.remaining() < keyLen + valueLen) {
            throw new IllegalArgumentException("Buffer remaining data is less than keyLen + valueLen");
        }

        byte[] keyBytes = new byte[keyLen];
        buffer.get(keyBytes);

        byte[] valueBytes = new byte[valueLen];
        buffer.get(valueBytes);

        WalDataStruct walDataStruct = new WalDataStruct(magic, keyLen, valueLen, checksum, keyBytes, valueBytes);

        // 校验 CRC32 校验码，如果发生磁道/数据损坏则抛出异常
        if (!walDataStruct.validateChecksum()) {
            throw new IllegalStateException("Data validation failed: Checksum mismatch (data might be corrupted)");
        }

        return walDataStruct;
    }

}
