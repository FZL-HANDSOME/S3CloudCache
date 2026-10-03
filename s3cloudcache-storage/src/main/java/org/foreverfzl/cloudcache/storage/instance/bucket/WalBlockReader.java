package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/** 先完整校验一个 Block，再交给恢复流程；损坏的前缀不能被当作完整 Block 上传。 */
/**
 * 中文：无状态 WAL 解码器，自动/人工恢复共用；按现有本机字节序读取，CRC 校验 Value，不修复损坏或确认上传。
 * English: Stateless decoder shared by automatic/manual recovery; reads existing native byte order and CRC-checks Value, without repairing damage or acknowledging uploads.
 * 中文：每次 O(BlockSize) 扫描并分配记录数组；输入须在整个调用期间稳定、存活且可访问。
 * English: Each call scans O(BlockSize) and allocates record arrays; input must remain stable, alive and accessible throughout.
 */
public final class WalBlockReader {
    /** 中文：工具类禁止实例化；English: utility class with no instances. */
    private WalBlockReader() { }

    /**
     * 中文：解析结果的浅不可变载体；value 是新建但仍可变的数组，消费者不得修改共享解析结果。
     * English: Shallowly immutable parse result; value is newly allocated but mutable, so consumers must not mutate shared results.
     * @param walOffset 中文：WAL Block 内记录头的字节偏移，不是对象偏移；English: record-header byte offset within the WAL block, not object offset.
     * @param value 中文：不含协议头和填充的 Value 字节；English: Value bytes excluding header/padding.
     */
    public record Record(long walOffset, byte[] value) { }

    /**
     * 中文：取得逻辑块视图后解码；本方法不 hold 文件，调用方必须持引用/文件锁避免清理。
     * English: Decodes a logical-block view without holding the file; caller must retain a reference/file lock against cleanup.
     * @param file 中文：有效来源 WAL；English: live source WAL.
     * @param blockIndex 中文：文件内合法块下标；English: valid block index in the file.
     * @return 中文：全块通过校验的记录列表，空块返回空列表；English: fully validated records, empty for an empty block.
     * @throws RuntimeException 中文：视图范围/生命周期非法或协议校验失败；English: invalid range/lifetime or protocol validation failure.
     */
    public static List<Record> readBlock(DefaultMappedFile file, int blockIndex) {
        return readBlock(file.getBlockMappedMemorySegmentSlice(blockIndex));
    }

    /** 自动恢复与人工读取共用同一校验规则，避免人工确认时漏掉 CRC 错误或零洞。 */
    /**
     * 中文：协议为 4 字节 Magic、4 字节 CRC、4 字节长度及 4 字节对齐的 Value；显式结束或不足 4 字节停止。
     * English: Format is four-byte Magic, CRC and length followed by four-byte-aligned Value; explicit end or fewer than four remaining bytes stops parsing.
     * 中文：兼容全零尾部，但零洞后仍有非零字节视为损坏；不是遇错误就返回有效前缀。
     * English: Accepts all-zero tails, but a zero gap followed by nonzero bytes is corruption, never a valid partial-prefix result.
     * @param segment 中文：整个逻辑块的稳定可读视图，不转移所有权；English: stable readable whole-block view, without ownership transfer.
     * @return 中文：按 WAL 顺序的新列表/数组，调用方负责不破坏共享结果；English: new list/arrays in WAL order; callers must preserve shared results.
     * @throws IllegalStateException 中文：非法 Magic/长度、截断头、CRC 或零洞错误；English: invalid Magic/length, truncated header, CRC or zero-gap error.
     */
    public static List<Record> readBlock(MemorySegment segment) {
        long end = segment.byteSize();
        List<Record> records = new ArrayList<>();
        CRC32 crc = new CRC32();
        for (long position = 0; end - position >= Integer.BYTES;) {
            int magic = segment.get(ValueLayout.JAVA_INT_UNALIGNED, position);
            // 中文：UNALIGNED 同时支持堆数组视图，不额外要求底层地址满足 int 对齐。
            // English: UNALIGNED also supports heap-array views without requiring an int-aligned address.
            // 兼容旧 WAL 的零填充尾部，以及显式 Padding 结束标志。
            // English: Accept legacy zero tails and the explicit padding marker; bytes after an explicit marker are intentionally not parsed.
            if (magic == DataStruct.END_MAGIC_NUMBER) break;
            if (magic == 0) {
                // 并发预留后崩溃可能留下“零洞 + 后续有效记录”，不能把前缀提交为整块。
                // English: A crash after concurrent reservation may leave a zero gap followed by valid records; do not commit only the prefix.
                long tail = position;
                // 大部分尾部是零填充，按 8 字节扫描，避免恢复大 WAL 时逐字节调用 FFM。
                // English: Scan common zero tails eight bytes at a time, then check remaining bytes individually.
                for (; end - tail >= Long.BYTES; tail += Long.BYTES) {
                    if (segment.get(ValueLayout.JAVA_LONG_UNALIGNED, tail) != 0) {
                        throw new IllegalStateException("Nonempty WAL after zero gap at block offset " + position);
                    }
                }
                for (; tail < end; tail++) {
                    if (segment.get(ValueLayout.JAVA_BYTE, tail) != 0) {
                        throw new IllegalStateException("Nonempty WAL after zero gap at block offset " + position);
                    }
                }
                break;
            }
            if (magic != DataStruct.MAGIC_NUMBER || end - position < DataStruct.HEADER_LENGTH) {
                throw new IllegalStateException("Invalid WAL record at block offset " + position);
            }
            int checksum = segment.get(ValueLayout.JAVA_INT_UNALIGNED, position + 4);
            int length = segment.get(ValueLayout.JAVA_INT_UNALIGNED, position + 8);
            long serializedLength = DataStruct.HEADER_LENGTH + ((length + 3L) & ~3L);
            // 中文：先用 long 对齐并做范围检查，再分配数组，防止负长度或 int 溢出绕过校验。
            // English: Align using long and validate bounds before allocation, preventing negative lengths or int overflow from bypassing checks.
            if (length <= 0 || serializedLength > end - position) {
                throw new IllegalStateException("Invalid WAL length at block offset " + position);
            }
            byte[] value = segment.asSlice(position + DataStruct.HEADER_LENGTH, length).toArray(ValueLayout.JAVA_BYTE);
            crc.reset();
            // 中文：CRC 按记录独立计算，不包含前条记录、协议头或对齐填充。
            // English: CRC is per record and excludes previous records, the header and alignment padding.
            crc.update(value);
            if ((int) crc.getValue() != checksum) {
                throw new IllegalStateException("WAL checksum mismatch at block offset " + position);
            }
            records.add(new Record(position, value));
            position += serializedLength;
        }
        return records;
    }
}
