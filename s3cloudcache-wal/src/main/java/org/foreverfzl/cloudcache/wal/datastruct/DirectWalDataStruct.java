package org.foreverfzl.cloudcache.wal.datastruct;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * 针对堆外内存的持久化协议
 */
/**
 * 中文：将MemorySegment借用视图序列化为WAL记录；构造时计算CRC但不拷贝源内容，调用方必须保持源内容稳定且内存有效直到writeTo结束。
 * English: Serializes a borrowed MemorySegment view as a WAL record. Construction computes CRC without copying the source; callers must keep the source stable and live through writeTo.
 */
public class DirectWalDataStruct implements DataStruct {

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
     * 中文：借用源segment，不拥有Arena，也不延长其生命周期。
     * English: Borrowed source segment; it neither owns nor extends the arena lifetime.
     */
    private final MemorySegment dataSegment;

    //堆外指定区域数据
    /**
     * 中文：借用指定内存范围并立即计算CRC；不转移Arena所有权，范围及线程访问限制由内存API检查。
     * English: Borrows the selected memory range and computes CRC immediately without taking arena ownership; memory APIs enforce bounds and thread-access constraints.
     *
     * @param dataSegment 中文：源segment，需保持有效；English: source segment that must remain live
     * @param fromOffset 中文：源偏移，字节；English: source offset in bytes
     * @param dataLen 中文：Value长度，字节；English: value length in bytes
     */
    public DirectWalDataStruct(MemorySegment dataSegment, int fromOffset,int dataLen) {
        this.dataSegment = dataSegment;
        this.dataLen = dataLen;
        this.fromOffset = fromOffset;
        magic = MAGIC_NUMBER;
        checksum = calculateCRC32(dataSegment, fromOffset, dataLen);
    }

    //堆外全部数据
    /**
     * 中文：借用整个segment；实现将byteSize强转为int，调用方应保证大小适合int与单个Block，构造器不额外作大段保护。
     * English: Borrows the whole segment. byteSize is cast to int, so callers must ensure it fits int and one block; this constructor adds no oversized-segment guard.
     *
     * @param dataSegment 中文：可访问且大小符合上层限制的源段；English: accessible source segment within caller size limits
     */
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
     * 计算数据区域的 CRC32 校验和 (Zero-Copy 零拷贝高性能版)
     */
    /**
     * 中文：通过segment切片的ByteBuffer计算CRC；无需复制完整Value到新数组，但视图对象/加速效果依赖JVM，不保证零分配或固定倍数提升。
     * English: Computes CRC through a segment slice's ByteBuffer without copying the entire value to a new array. View allocation and acceleration depend on the JVM; no zero-allocation or fixed speedup is guaranteed.
     *
     * @param dataSegment 中文：借用源数据；English: borrowed source data
     * @param fromOffset 中文：源起点，字节；English: source offset in bytes
     * @param dataLen 中文：Value长度，字节；English: value length in bytes
     * @return 中文：CRC32的int位模式；English: CRC32 as an int bit pattern
     */
    private static int calculateCRC32(MemorySegment dataSegment, int fromOffset, int dataLen) {
        if (dataLen == 0) return 0;
        CRC32 crc = new CRC32();
        // 利用 asSlice + asByteBuffer 将堆外 Segment 零拷贝转换为 DirectByteBuffer
        // 触发 JVM Intrinsics 向量化 C++ 原生 CRC32 指令，零堆内存分配，速度提升 10~50 倍
        // 中文：更正上方性能描述：这里避免Value整体数组复制，但视图对象分配和CRC加速取决于JDK/平台，未承诺10到50倍性能。
        // English: Clarifies the performance claim above: this avoids copying the whole value into an array, but view allocation and CRC acceleration depend on the JDK/platform; no 10-to-50-fold speedup is promised.
        ByteBuffer byteBuffer = dataSegment.asSlice(fromOffset, dataLen).asByteBuffer();
        crc.update(byteBuffer);
        return (int) crc.getValue();
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
        int computed = calculateCRC32(this.dataSegment, this.fromOffset, this.dataLen);
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
        MemorySegment.copy(dataSegment, ValueLayout.JAVA_BYTE, fromOffset, target, ValueLayout.JAVA_BYTE, pos, dataLen);
    }
}
