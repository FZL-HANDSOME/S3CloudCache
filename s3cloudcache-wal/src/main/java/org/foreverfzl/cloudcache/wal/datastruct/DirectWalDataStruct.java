package org.foreverfzl.cloudcache.wal.datastruct;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.zip.CRC32;

/**
 * 针对堆外内存的持久化协议
 */
public class DirectWalDataStruct implements DataStruct {

    private final int magic;
    private final int checksum;
    private final int fromOffset;
    private final int dataLen;
    private final MemorySegment dataSegment;

    //堆外指定区域数据
    public DirectWalDataStruct(MemorySegment dataSegment, int dataLen, int fromOffset) {
        this.dataSegment = dataSegment;
        this.dataLen = dataLen;
        this.fromOffset = fromOffset;
        magic = MAGIC_NUMBER;
        checksum = calculateCRC32(dataSegment, fromOffset, dataLen);
    }

    //堆外全部数据
    public DirectWalDataStruct(MemorySegment dataSegment) {
        this.dataSegment = dataSegment;
        this.dataLen = (int) dataSegment.byteSize();
        this.fromOffset = 0;
        this.magic = MAGIC_NUMBER;
        checksum = calculateCRC32(dataSegment, fromOffset, dataLen);
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
    private static int calculateCRC32(MemorySegment dataSegment,int fromOffset,int dataLen){
        CRC32 crc=new CRC32();
        byte[] buffer=new byte[8192];
        long offset=fromOffset;
        int remain=dataLen;
        while(remain>0){
            int len=Math.min(remain, buffer.length);
            MemorySegment.copy(
                    dataSegment,
                    ValueLayout.JAVA_BYTE,
                    offset,
                    buffer,
                    0,
                    len
            );
            crc.update(
                    buffer,
                    0,
                    len
            );
            offset+=len;
            remain-=len;
        }
        return (int)crc.getValue();
    }

    /**
     * 校验当前数据的校验和是否正确。
     */
    private boolean validateChecksum() {
        int computed = calculateCRC32(this.dataSegment, this.fromOffset, this.dataLen);
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
                dataSegment,
                fromOffset,
                target,
                ValueLayout.JAVA_BYTE,
                pos,
                dataLen
        );
    }
}
