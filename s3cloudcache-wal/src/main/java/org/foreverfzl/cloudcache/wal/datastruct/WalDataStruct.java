package org.foreverfzl.cloudcache.wal.datastruct;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * 磁盘持久化协议格式
 */
public final class WalDataStruct implements DataStruct{

    private final int magic;
    private final int checksum;
    private final int fromOffset;
    private final int dataLen;
    private final byte[] dataBytes;

    //默认添加全部数据
    public WalDataStruct(byte[] dataBytes) {
        if (dataBytes == null) {
            throw new IllegalArgumentException("Value bytes cannot be null");
        }
        this.magic = MAGIC_NUMBER;
        this.dataLen = dataBytes.length;
        this.dataBytes = dataBytes;
        this.fromOffset=0;
        this.checksum = calculateCRC32(dataBytes,fromOffset,dataLen);
    }

    //添加指定数据
    public WalDataStruct(byte[] dataBytes,int fromOffset, int dataLen) {
        if (dataBytes == null) {
            throw new IllegalArgumentException("Value bytes cannot be null");
        }
        this.magic=MAGIC_NUMBER;
        this.fromOffset = fromOffset;
        this.dataLen = dataLen;
        this.dataBytes = dataBytes;
        this.checksum = calculateCRC32(dataBytes,fromOffset,dataLen);
    }


    public WalDataStruct(int magic, int checksum, int fromOffset, int dataLen, byte[] dataBytes) {
        this.magic = magic;
        this.checksum = checksum;
        this.fromOffset = fromOffset;
        this.dataLen = dataLen;
        this.dataBytes = dataBytes;
    }

    /**
     * 获取当前数据包序列化后的总字节数 (Header + Key + Value)，4字节对齐
     */
    @Override
    public long getSerializedSize() {
        long size = HEADER_LENGTH + dataLen;
        return (size + 3) & ~3;
    }

    /**
     * 计算数据区域的CRC32校验和
     */
    private static int calculateCRC32(byte[] dataBytes,int fromOffset,int dataLen) {
        CRC32 crc=new CRC32();
        crc.update(dataBytes,fromOffset,dataLen);
        return (int)crc.getValue();
    }

    /**
     * 校验当前数据的校验和是否正确。
     */
    private boolean validateChecksum() {
        int computed = calculateCRC32(this.dataBytes,this.fromOffset,this.dataLen);
        return this.checksum == computed;
    }

    /**
     * 校验魔数是否匹配。
     */
    public boolean validateMagic() {
        return this.magic == MAGIC_NUMBER;
    }


    @Override
    public void writeTo(MemorySegment target) {
        long pos=0;

        // 1. Magic
        target.set(
                ValueLayout.JAVA_INT,
                pos,
                magic
        );
        pos+=4;


        // 3. Checksum
        target.set(
                ValueLayout.JAVA_INT,
                pos,
                checksum
        );
        pos+=4;

        // 4. Data Length
        target.set(
                ValueLayout.JAVA_INT,
                pos,
                dataLen
        );
        pos+=4;

        // 5. Data Bytes
        MemorySegment.copy(
                dataBytes,
                fromOffset,
                target,
                ValueLayout.JAVA_BYTE,
                pos,
                dataLen
        );
    }
}
