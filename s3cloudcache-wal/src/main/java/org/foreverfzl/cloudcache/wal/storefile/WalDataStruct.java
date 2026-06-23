package org.foreverfzl.cloudcache.wal.storefile;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * 磁盘持久化协议格式
 */
public final class WalDataStruct {

    // 固定魔数 0x53334343 (ASCII "S3CC")
    public static final int MAGIC_NUMBER = 0x53334343;

    // 协议头部长度固定为 16 字节 (4 + 4 + 4 + 4)
    public static final long HEADER_LENGTH = 16;

    private final int magic;
    private final int version;
    private final int checksum;
    private final int valueLen;
    private final byte[] valueBytes;

    /**
     * 构造一个新的 WalDataStruct，自动计算字段长度及 CRC32 校验和。
     *
     * @param valueBytes 业务 Value 的字节数组
     */
    public WalDataStruct(byte[] valueBytes) {
        if (valueBytes == null) {
            throw new IllegalArgumentException("Value bytes cannot be null");
        }
        this.magic = MAGIC_NUMBER;
        this.version = 1;
        this.valueLen = valueBytes.length;
        this.valueBytes = valueBytes;
        this.checksum = calculateCRC32(valueBytes);
    }


    public WalDataStruct(String value) {
        this(value != null ? value.getBytes(StandardCharsets.UTF_8) : null);
    }

    public WalDataStruct(int magic, int version, int valueLen, int checksum, byte[] valueBytes) {
        this.magic = magic;
        this.version = version;
        this.valueLen = valueLen;
        this.checksum = checksum;
        this.valueBytes = valueBytes;
    }

    public int getMagic() {
        return magic;
    }

    public int getVersion() {
        return version;
    }

    public int getValueLen() {
        return valueLen;
    }

    public int getChecksum() {
        return checksum;
    }

    public byte[] getValueBytes() {
        return valueBytes;
    }

    /**
     * 获取当前数据包序列化后的总字节数 (Header + Key + Value)，4字节对齐
     */
    public long getSerializedSize() {
        long size = HEADER_LENGTH + valueLen;
        return (size + 3) & ~3;
    }

    /**
     * 计算 Key 和 Value 联合的 CRC32 校验和。
     */
    private static int calculateCRC32(byte[] valueBytes) {
        CRC32 crc = new CRC32();
        if (valueBytes != null && valueBytes.length > 0) {
            crc.update(valueBytes);
        }
        return (int) crc.getValue();
    }

    /**
     * 校验当前数据的校验和是否正确。
     */
    private boolean validateChecksum() {
        long computed = calculateCRC32(this.valueBytes);
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
        buffer.putInt(magic);
        buffer.putInt(version);
        buffer.putInt(checksum);
        buffer.putInt(valueLen);
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
        int magic = buffer.getInt();
        if (magic != MAGIC_NUMBER) {
            throw new IllegalArgumentException(
                    String.format("Invalid magic number:0x%08X", magic)
            );
        }
        int version = buffer.getInt();
        int checksum = buffer.getInt();
        int valueLen = buffer.getInt();
        if (buffer.remaining() < valueLen) {
            throw new IllegalArgumentException(
                    "Buffer remaining data is less than valueLen"
            );
        }
        byte[] valueBytes = new byte[valueLen];
        buffer.get(valueBytes);
        WalDataStruct walDataStruct =
                new WalDataStruct(
                        magic,
                        version,
                        valueLen,
                        checksum,
                        valueBytes
                );
        if (!walDataStruct.validateChecksum()) {
            throw new IllegalStateException(
                    "Data validation failed: Checksum mismatch"
            );
        }
        return walDataStruct;
    }

}
