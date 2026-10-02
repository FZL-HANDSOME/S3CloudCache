package org.foreverfzl.cloudcache.storage.instance.bucket;

import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/** 先完整校验一个 Block，再交给恢复流程；损坏的前缀不能被当作完整 Block 上传。 */
public final class WalBlockReader {
    private WalBlockReader() { }

    public record Record(long walOffset, byte[] value) { }

    public static List<Record> readBlock(DefaultMappedFile file, int blockIndex) {
        return readBlock(file.getBlockMappedMemorySegmentSlice(blockIndex));
    }

    /** 自动恢复与人工读取共用同一校验规则，避免人工确认时漏掉 CRC 错误或零洞。 */
    public static List<Record> readBlock(MemorySegment segment) {
        long end = segment.byteSize();
        List<Record> records = new ArrayList<>();
        CRC32 crc = new CRC32();
        for (long position = 0; end - position >= Integer.BYTES;) {
            int magic = segment.get(ValueLayout.JAVA_INT_UNALIGNED, position);
            // 兼容旧 WAL 的零填充尾部，以及显式 Padding 结束标志。
            if (magic == DataStruct.END_MAGIC_NUMBER) break;
            if (magic == 0) {
                // 并发预留后崩溃可能留下“零洞 + 后续有效记录”，不能把前缀提交为整块。
                long tail = position;
                // 大部分尾部是零填充，按 8 字节扫描，避免恢复大 WAL 时逐字节调用 FFM。
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
            if (length <= 0 || serializedLength > end - position) {
                throw new IllegalStateException("Invalid WAL length at block offset " + position);
            }
            byte[] value = segment.asSlice(position + DataStruct.HEADER_LENGTH, length).toArray(ValueLayout.JAVA_BYTE);
            crc.reset();
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
