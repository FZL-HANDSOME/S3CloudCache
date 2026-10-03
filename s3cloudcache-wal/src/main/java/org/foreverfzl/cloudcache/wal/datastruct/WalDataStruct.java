package org.foreverfzl.cloudcache.wal.datastruct;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.zip.CRC32;

/**
 * 针对堆内数据
 */
/**
 * 中文：将堆内字节数组序列化为WAL记录；构造时计算CRC但不拷贝源内容，调用方必须保持源内容稳定且内存有效直到writeTo结束。
 * English: Serializes a heap byte array as a WAL record. Construction computes CRC without copying the source; callers must keep the source stable and live through writeTo.
 */
public final class WalDataStruct implements DataStruct{

    /**
     * 中文：写入记录头的魔数，不参与Value CRC。
     * English: Magic stored in the record header; it is not included in the value CRC.
     */
    private final int magic;
    /**
     * 中文：构造时对指定Value范围计算的CRC32，低32位保存在int中。
     * English: CRC32 of the selected value range at construction, stored as an int bit pattern.
     */
    private final int checksum;
    /**
     * 中文：源数据起点，单位字节；不是WAL偏移。
     * English: Source start in bytes, not a WAL offset.
     */
    private final int fromOffset;
    /**
     * 中文：Value长度，单位字节，不含12字节头和对齐。
     * English: Value length in bytes, excluding the 12-byte header and alignment.
     */
    private final int dataLen;
    /**
     * 中文：借用源数组，不做防御性复制。
     * English: Borrowed source array without defensive copying.
     */
    private final byte[] dataBytes;

    //默认添加全部数据
    /**
     * 中文：借用整个非null数组并立即计算CRC。
     * English: Borrows the entire non-null array and computes CRC immediately.
     *
     * @param dataBytes 中文：源数组，写入结束前不要修改；English: source array; do not mutate before writing finishes
     * @throws IllegalArgumentException 中文：源数组为null；English: source array is null
     */
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
    /**
     * 中文：借用数组子范围；构造时计算CRC，源内容随后仍必须保持稳定。
     * English: Borrows an array subrange and computes CRC at construction; source contents must remain stable afterwards.
     *
     * @param dataBytes 中文：源数组；English: source array
     * @param fromOffset 中文：源偏移，字节；English: source offset in bytes
     * @param dataLen 中文：Value长度，字节；English: value length in bytes
     */
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


    /**
     * 中文：直接保存外部提供的记录字段，不计算CRC也不验证范围；用于需要显式控制记录头的调用方。
     * English: Stores externally supplied fields without computing CRC or validating bounds, for callers that explicitly control the record header.
     *
     * @param magic 中文：待写入的魔数；English: magic to serialize
     * @param checksum 中文：调用者已计算的CRC32；English: caller-supplied CRC32
     * @param fromOffset 中文：源偏移，字节；English: source offset in bytes
     * @param dataLen 中文：Value长度，字节；English: value length in bytes
     * @param dataBytes 中文：借用源数组；English: borrowed source array
     */
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
    /**
     * 中文：更正旧描述：序列化内容没有Key；返回12字节头加Value后向上对齐4字节的大小。
     * English: Clarifies the historical description: there is no key; returns header plus value rounded up to four bytes.
     *
     * @return 中文：WAL空间预留字节数；English: WAL reservation size in bytes
     */
    @Override
    public long getSerializedSize() {
        long size = HEADER_LENGTH + dataLen;
        return (size + 3) & ~3;
    }

    /**
     * 中文：返回纯Value长度。
     * English: Returns value-only length.
     *
     * @return 中文：Value字节数；English: value byte count
     */
    @Override
    public int getDataLen() {
        return dataLen;
    }

    /**
     * 计算数据区域的CRC32校验和
     */
    /**
     * 中文：对源数组的指定范围计算CRC；范围校验由CRC API执行。
     * English: Computes CRC for the selected source-array range; the CRC API validates the range.
     *
     * @param dataBytes 中文：借用源数据；English: borrowed source data
     * @param fromOffset 中文：源起点，字节；English: source offset in bytes
     * @param dataLen 中文：Value长度，字节；English: value length in bytes
     * @return 中文：CRC32的int位模式；English: CRC32 as an int bit pattern
     */
    private static int calculateCRC32(byte[] dataBytes,int fromOffset,int dataLen) {
        CRC32 crc=new CRC32();
        crc.update(dataBytes,fromOffset,dataLen);
        return (int)crc.getValue();
    }

    /**
     * 校验当前数据的校验和是否正确。
     */
    /**
     * 中文：重新计算当前源数据的CRC并比较；该辅助方法不会自动在writeTo中调用。
     * English: Recomputes source CRC for comparison; writeTo does not automatically invoke this helper.
     *
     * @return 中文：当前内容是否匹配构造时CRC；English: whether current contents match the stored CRC
     */
    private boolean validateChecksum() {
        int computed = calculateCRC32(this.dataBytes,this.fromOffset,this.dataLen);
        return this.checksum == computed;
    }

    /**
     * 校验魔数是否匹配。
     */
    /**
     * 中文：仅比较记录魔数，不验证源数据完整性。
     * English: Checks only the magic, not source-data integrity.
     *
     * @return 中文：魔数是否为正常记录标志；English: whether the normal-record marker matches
     */
    public boolean validateMagic() {
        return this.magic == MAGIC_NUMBER;
    }


    /**
     * 中文：依次写入本机字节序的三个int和Value；不主动写对齐填充、结束标志或force，目标范围由上层预留。
     * English: Writes three native-order ints followed by the value. It does not explicitly write alignment padding or an end marker, nor force; the caller reserves the target range.
     *
     * @param target 中文：有效、可写且足够大的目标切片；English: live, writable target slice of sufficient size
     */
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
