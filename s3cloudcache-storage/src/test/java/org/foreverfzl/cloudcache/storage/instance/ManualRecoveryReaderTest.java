package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.metadata.entity.DeadDataInfo;
import org.foreverfzl.cloudcache.storage.instance.bucket.MappedFileReader;
import org.foreverfzl.cloudcache.storage.instance.bucket.WalBlockReader;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.datastruct.WalDataStruct;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

/** 人工恢复快照回归：仅创建 8KB 临时 WAL，不调用 S3，也不接触用户目录。 */
public class ManualRecoveryReaderTest {
    private static final int BLOCK_SIZE = 4096;
    private static final byte[] FIRST = {1, 2, 3};
    private static final byte[] SECOND = {4, 5, 6, 7, 8};
    private static final byte[] THIRD = {9, 10, 11, 12, 13, 14, 15};

    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void readAllReturnsWholeBlockValuesInCompactReadOnlyNativeMemory() throws Exception {
        try (WalFixture fixture = fixture(FIRST, SECOND)) {
            fixture.manager.sealAllBlocks();
            try (MappedFileReader reader = fixture.reader()) {
                byte[] returned = reader.next();
                assertArrayEquals(FIRST, returned);
                returned[0] = 99; // 调用方修改 next 返回数组，不得改变内部校验结果或 readAll。
                MemorySegment values = reader.readAll();
                assertTrue(values.isNative());
                assertTrue(values.isReadOnly());
                assertEquals(FIRST.length + SECOND.length, values.byteSize());
                assertArrayEquals(join(FIRST, SECOND), values.toArray(ValueLayout.JAVA_BYTE));
                assertThrows(IllegalArgumentException.class, () -> values.set(ValueLayout.JAVA_BYTE, 0, (byte) 0));
                assertFalse(reader.hasNext());
                assertEquals(serialized(FIRST) + serialized(SECOND), reader.getCurPosition());
                assertSame(values, reader.readAll());
                assertEquals(BLOCK_SIZE, reader.getMemorySegment().byteSize());
                assertTrue(reader.getMemorySegment().isReadOnly());
            }
        }
    }

    @Test
    public void iteratorAndSeekingOnlyUseRecordBoundaries() throws Exception {
        try (WalFixture fixture = fixture(FIRST, SECOND); MappedFileReader reader = fixture.reader()) {
            assertEquals(0, reader.getCurPosition());
            assertTrue(reader.hasNext());
            assertTrue(reader.hasNext());
            assertEquals(0, reader.getCurPosition());
            assertArrayEquals(FIRST, reader.next());
            assertEquals(serialized(FIRST), reader.getCurPosition());
            assertArrayEquals(SECOND, reader.next());
            assertFalse(reader.hasNext());
            assertThrows(NoSuchElementException.class, reader::next);

            reader.setCurPosition(serialized(FIRST));
            assertArrayEquals(SECOND, reader.next());
            reader.setCurPosition(0);
            assertArrayEquals(FIRST, reader.next());
            long dataEnd = serialized(FIRST) + serialized(SECOND);
            reader.setCurPosition(dataEnd);
            assertFalse(reader.hasNext());
            reader.setCurPosition(BLOCK_SIZE);
            assertFalse(reader.hasNext());
            assertThrows(IllegalArgumentException.class, () -> reader.setCurPosition(-1));
            assertThrows(IllegalArgumentException.class, () -> reader.setCurPosition(1));
            assertThrows(IllegalArgumentException.class, () -> reader.setCurPosition(DataStruct.HEADER_LENGTH));
            assertThrows(IllegalArgumentException.class, () -> reader.setCurPosition(dataEnd + 4));
            assertThrows(IllegalArgumentException.class, () -> reader.setCurPosition(BLOCK_SIZE + 1L));
            // readAll 的“整块”语义也适用于已定位至块末尾的游标。
            assertArrayEquals(join(FIRST, SECOND), reader.readAll().toArray(ValueLayout.JAVA_BYTE));
        }
    }

    @Test
    public void snapshotSurvivesWalCleanupButExpiresWithReader() throws Exception {
        try (WalFixture fixture = fixture(FIRST, SECOND)) {
            MappedFileReader reader = fixture.reader();
            MemorySegment rawSnapshot = reader.getMemorySegment();
            MemorySegment values;
            try {
                fixture.file.clean();
                fixture.close();
                assertTrue(fixture.file.isCleanup());
                // 首次解析发生在原映射卸载之后，证明 Reader 不借用源 WAL 内存。
                assertArrayEquals(FIRST, reader.next());
                values = reader.readAll();
                assertArrayEquals(join(FIRST, SECOND), values.toArray(ValueLayout.JAVA_BYTE));
                assertTrue(rawSnapshot.scope().isAlive());
                assertThrows(RuntimeException.class, reader::ackUpLoadPosition);
                assertEquals(0, fixture.file.upLoadPosition);
            } finally {
                reader.close();
            }
            assertFalse(rawSnapshot.scope().isAlive());
            assertThrows(IllegalStateException.class, () -> rawSnapshot.get(ValueLayout.JAVA_BYTE, 0));
            assertThrows(IllegalStateException.class, () -> values.get(ValueLayout.JAVA_BYTE, 0));
            assertThrows(IllegalStateException.class, reader::hasNext);
            assertThrows(IllegalStateException.class, reader::next);
            assertThrows(IllegalStateException.class, reader::readAll);
            assertThrows(IllegalStateException.class, reader::ackUpLoadPosition);
            assertThrows(IllegalStateException.class, reader::getMemorySegment);
            assertThrows(IllegalStateException.class, () -> reader.setCurPosition(0));
            reader.close(); // 重复 close 必须是幂等操作。
        }
    }

    @Test
    public void readingAndClosingNeverAcknowledgeWal() throws Exception {
        try (WalFixture fixture = fixture(FIRST)) {
            Path walFile = fixture.directory.resolve("wal").resolve(fixture.file.getFileName());
            try (MappedFileReader reader = fixture.reader()) {
                assertArrayEquals(FIRST, reader.readAll().toArray(ValueLayout.JAVA_BYTE));
                assertEquals(0, fixture.file.upLoadPosition);
            }
            fixture.file.close();
            assertEquals(0, fixture.file.upLoadPosition);
            assertFalse(fixture.file.isBlockUploaded(0));
            assertFalse(fixture.file.canClean());
            assertTrue(Files.exists(walFile));
        }
    }

    @Test
    public void explicitAcknowledgementValidatesAndAdvancesExactlyOneBlock() throws Exception {
        try (WalFixture fixture = fixture(FIRST, SECOND); MappedFileReader reader = fixture.reader()) {
            fixture.manager.sealAllBlocks();
            reader.ackUpLoadPosition();
            assertTrue(fixture.file.isBlockUploaded(0));
            assertFalse(fixture.file.isBlockUploaded(1));
            assertEquals(BLOCK_SIZE, fixture.file.upLoadPosition);
            reader.ackUpLoadPosition();
            assertEquals(BLOCK_SIZE, fixture.file.upLoadPosition);
            // 确认不移动读取游标，快照仍可继续读取。
            assertArrayEquals(FIRST, reader.next());
        }
    }

    @Test
    public void unsealedBlockCannotBeAcknowledgedEvenWhenItsCurrentBytesAreValid() throws Exception {
        try (WalFixture fixture = fixture(FIRST); MappedFileReader reader = fixture.reader()) {
            assertArrayEquals(FIRST, reader.readAll().toArray(ValueLayout.JAVA_BYTE));
            assertThrows(IllegalStateException.class, reader::ackUpLoadPosition);
            assertEquals(0, fixture.file.upLoadPosition);
            assertFalse(fixture.file.isBlockUploaded(0));
        }
    }

    @Test
    public void zeroedTailRecordCannotBeMistakenForACompleteSnapshot() throws Exception {
        try (WalFixture fixture = fixture(FIRST, SECOND)) {
            // 元数据登记了两条记录，但尾部第二条全部为零；与中间零洞不同，协议扫描会只得到第一条。
            fixture.block().asSlice(serialized(FIRST), serialized(SECOND)).fill((byte) 0);
            assertEquals(1, WalBlockReader.readBlock(fixture.file, 0).size());
            try (MappedFileReader reader = fixture.reader()) {
                assertThrows(IllegalStateException.class, reader::hasNext);
                assertThrows(IllegalStateException.class, reader::readAll);
                assertThrows(IllegalStateException.class, reader::ackUpLoadPosition);
            }
            assertEquals(0, fixture.file.upLoadPosition);
            assertFalse(fixture.file.isBlockUploaded(0));
        }
    }

    @Test
    public void crcDamageRejectsWholeBlockBeforeReturningValidPrefix() throws Exception {
        try (WalFixture fixture = fixture(FIRST, SECOND)) {
            MemorySegment block = fixture.block();
            long checksumOffset = serialized(FIRST) + 4;
            int checksum = block.get(ValueLayout.JAVA_INT_UNALIGNED, checksumOffset);
            block.set(ValueLayout.JAVA_INT_UNALIGNED, checksumOffset, checksum ^ 1);
            assertRejectedWithoutAcknowledgement(fixture);
        }
    }

    @Test
    public void illegalMagicRejectsWholeBlock() throws Exception {
        try (WalFixture fixture = fixture(FIRST, SECOND)) {
            fixture.block().set(ValueLayout.JAVA_INT_UNALIGNED, serialized(FIRST), 0x12345678);
            assertRejectedWithoutAcknowledgement(fixture);
        }
    }

    @Test
    public void invalidLengthsCannotBeAcknowledged() throws Exception {
        for (int length : new int[]{-1, 0, Integer.MAX_VALUE, BLOCK_SIZE}) {
            try (WalFixture fixture = fixture(FIRST, SECOND)) {
                fixture.block().set(ValueLayout.JAVA_INT_UNALIGNED, serialized(FIRST) + 8, length);
                assertRejectedWithoutAcknowledgement(fixture);
            }
        }
    }

    @Test
    public void zeroReservationHoleCannotHideAValidLaterRecord() throws Exception {
        try (WalFixture fixture = fixture(FIRST, SECOND, THIRD)) {
            fixture.block().asSlice(serialized(FIRST), serialized(SECOND)).fill((byte) 0);
            assertRejectedWithoutAcknowledgement(fixture);
        }
    }

    @Test
    public void emptyBlockCanBeReadButNeverAcknowledged() throws Exception {
        try (WalFixture fixture = fixture(); MappedFileReader reader = fixture.reader()) {
            assertFalse(reader.hasNext());
            assertThrows(NoSuchElementException.class, reader::next);
            MemorySegment values = reader.readAll();
            assertTrue(values.isNative());
            assertTrue(values.isReadOnly());
            assertEquals(0, values.byteSize());
            reader.setCurPosition(0);
            reader.setCurPosition(BLOCK_SIZE);
            assertThrows(IllegalStateException.class, reader::ackUpLoadPosition);
            assertEquals(0, fixture.file.upLoadPosition);
        }
    }

    @Test
    public void sharedDecoderAcceptsLessThanFourTailBytesButRejectsTruncatedHeader() {
        byte[] bytes = new byte[Math.toIntExact(serialized(FIRST)) + 3];
        MemorySegment segment = MemorySegment.ofArray(bytes);
        // WAL 写入器要求四字节对齐；用堆外段构造真实协议，再复制到解码器的堆内输入。
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeRecord = arena.allocate(serialized(FIRST), Integer.BYTES);
            new WalDataStruct(FIRST).writeTo(nativeRecord);
            segment.asSlice(0, serialized(FIRST)).copyFrom(nativeRecord);
        }
        segment.asSlice(serialized(FIRST), 3).fill((byte) 0x7f);
        List<WalBlockReader.Record> records = WalBlockReader.readBlock(segment);
        assertEquals(1, records.size());
        assertArrayEquals(FIRST, records.getFirst().value());
        MemorySegment incompleteHeader = MemorySegment.ofArray(new byte[8]);
        incompleteHeader.set(ValueLayout.JAVA_INT_UNALIGNED, 0, DataStruct.MAGIC_NUMBER);
        assertThrows(IllegalStateException.class, () -> WalBlockReader.readBlock(incompleteHeader));
    }

    private static void assertRejectedWithoutAcknowledgement(WalFixture fixture) {
        assertThrows(IllegalStateException.class, () -> WalBlockReader.readBlock(fixture.file, 0));
        try (MappedFileReader reader = fixture.reader()) {
            assertThrows(IllegalStateException.class, reader::hasNext);
            assertThrows(IllegalStateException.class, reader::next);
            assertThrows(IllegalStateException.class, reader::readAll);
            assertThrows(IllegalStateException.class, reader::ackUpLoadPosition);
        }
        assertEquals(0, fixture.file.upLoadPosition);
        assertFalse(fixture.file.isBlockUploaded(0));
    }

    private WalFixture fixture(byte[]... values) throws Exception {
        WalFixture fixture = new WalFixture(temporary.newFolder().toPath());
        try {
            for (byte[] value : values) assertTrue(fixture.manager.appendData(new WalDataStruct(value)).isOk());
            return fixture;
        } catch (Throwable failure) {
            fixture.close();
            throw failure;
        }
    }

    private static long serialized(byte[] value) {
        return DataStruct.HEADER_LENGTH + ((value.length + 3L) & ~3L);
    }

    private static byte[] join(byte[]... values) {
        int total = 0;
        for (byte[] value : values) total += value.length;
        byte[] result = new byte[total];
        int position = 0;
        for (byte[] value : values) {
            System.arraycopy(value, 0, result, position, value.length);
            position += value.length;
        }
        return result;
    }

    private static final class WalFixture implements AutoCloseable {
        private final Path directory;
        private final MappedFileManager manager;
        private final DefaultMappedFile file;
        private boolean closed;

        private WalFixture(Path directory) {
            this.directory = directory;
            BucketConfig config = new BucketConfig().setS3KeyPrefix("manual-reader")
                    .setBlockSize(BLOCK_SIZE).setWalFileSize(2L * BLOCK_SIZE).setCacheSize(2L * BLOCK_SIZE)
                    .setWarmWalFile(false).setLockMappedFilePageCache(false);
            config.chackMappedFileTime = 60000;
            manager = new MappedFileManager(directory.toString(), "reader-test", "bucket", config, 0);
            manager.stopAllThread();
            file = manager.getActiveMappedFile().get();
        }

        private MemorySegment block() {
            return file.getBlockMappedMemorySegmentSlice(0);
        }

        private MappedFileReader reader() {
            return new MappedFileReader(file, block(), BLOCK_SIZE,
                    new DeadDataInfo("reader-test", "bucket", file.fileFromOffset, 0, "manual-reader/key"));
        }

        @Override
        public void close() {
            if (!closed) {
                manager.close();
                closed = true;
            }
        }
    }
}
