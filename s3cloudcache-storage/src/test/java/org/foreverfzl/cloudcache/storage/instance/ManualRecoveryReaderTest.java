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
/**
 * 中文：验证纯 Value 输出、整块校验、快照所有权与显式确认边界；测试中的直接 ack 仅检查本地协议，不代替真实 S3 成功确认。
 * English: Verifies Value-only output, whole-Block validation, snapshot ownership, and explicit acknowledgement; direct test acknowledgements check local behavior, not real S3 success.
 */
public class ManualRecoveryReaderTest {
    /**
     * 中文：4 KiB 逻辑块用于低资源协议测试，WAL 数据区含两块并另有文件头。
     * English: A 4 KiB logical block for low-resource protocol tests; the WAL data region has two blocks plus a separate file header.
     */
    private static final int BLOCK_SIZE = 4096;
    /**
     * 中文：3 字节 Value，序列化时会产生一字节对齐填充。
     * English: A 3-byte Value whose serialization requires one padding byte.
     */
    private static final byte[] FIRST = {1, 2, 3};
    /**
     * 中文：5 字节 Value，用于与协议头、对齐长度及损坏的第二条记录区分。
     * English: A 5-byte Value distinguishing payload length from headers/alignment and serving as the damaged second record.
     */
    private static final byte[] SECOND = {4, 5, 6, 7, 8};
    /**
     * 中文：位于人工零洞之后的有效记录，证明扫描不能把中间零洞当作正常尾部。
     * English: Valid data placed after an injected zero hole, proving a middle hole cannot be treated as a normal tail.
     */
    private static final byte[] THIRD = {9, 10, 11, 12, 13, 14, 15};

    /**
     * 中文：JUnit 创建并清理的隔离目录，所有映射必须在规则清理前关闭。
     * English: Isolated directory created and cleaned by JUnit; mappings must close before rule cleanup.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * 中文：先消费一条记录再 readAll，验证返回仍是整块纯 Value、精确有效长度、只读堆外视图且不受 next 数组修改影响。
     * English: Consumes one record before readAll, verifying whole-Block Values, exact exposed length, a read-only native view, and independence from a modified next array.
     * @throws Exception 中文：临时 WAL 创建或生命周期操作失败；English: temporary-WAL creation or lifecycle operations fail
     */
    @Test
    public void readAllReturnsWholeBlockValuesInCompactReadOnlyNativeMemory() throws Exception {
        try (WalFixture fixture = fixture(FIRST, SECOND)) {
            fixture.manager.sealAllBlocks();
            try (MappedFileReader reader = fixture.reader()) {
                byte[] returned = reader.next();
                assertArrayEquals(FIRST, returned);
                // 中文：修改调用方数组是别名隔离注入，不修改原 WAL，也不应改变缓存的整块解析结果。
                // English: Mutating the caller array probes alias isolation; it changes neither source WAL nor cached whole-Block parsing.
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

    /**
     * 中文：检查 hasNext 无副作用、next 的结束异常，以及 seek 只接受记录边界、数据末尾或块末尾。
     * English: Checks side-effect-free hasNext, next exhaustion, and seeks restricted to record boundaries, data end, or block end.
     * @throws Exception 中文：测试 WAL 创建或资源操作失败；English: test-WAL creation or resource operations fail
     */
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

    /**
     * 中文：在首次解析前卸载源 WAL 并关闭管理器，证明 Reader 持有独立快照；关闭 Reader 后视图及读取/确认入口失效。
     * English: Unmaps source WAL and closes its manager before first parsing to prove independent snapshot ownership; Reader close invalidates views and read/acknowledgement entry points.
     * @throws Exception 中文：测试 WAL 创建或关闭操作失败；English: test-WAL creation or closing fails
     */
    @Test
    public void snapshotSurvivesWalCleanupButExpiresWithReader() throws Exception {
        try (WalFixture fixture = fixture(FIRST, SECOND)) {
            MappedFileReader reader = fixture.reader();
            MemorySegment rawSnapshot = reader.getMemorySegment();
            MemorySegment values;
            try {
                // 中文：故意在 Reader 存活期间先释放源映射；后续读取必须只访问 Reader 自有 Arena。
                // English: Release the source mapping while the Reader lives; subsequent reads must use only the Reader-owned Arena.
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

    /**
     * 中文：读取并关闭快照不会确认上传；即便关闭文件写入口，未确认 WAL 仍不能清理。
     * English: Reading and closing a snapshot do not acknowledge upload; even closing file admission must leave unacknowledged WAL ineligible for cleanup.
     * @throws Exception 中文：测试 WAL 或文件状态检查失败；English: test-WAL or file-state operations fail
     */
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

    /**
     * 中文：封口后显式确认恰好推进当前一块，重复确认幂等且不消费 Reader 的迭代游标。
     * English: Explicit acknowledgement after sealing advances exactly one block, is idempotent, and does not consume the Reader cursor.
     * @throws Exception 中文：测试 WAL 创建或确认操作失败；English: test-WAL creation or acknowledgement fails
     */
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

    /**
     * 中文：当前字节合法不代表逻辑块已停止接收数据；OPEN 块必须拒绝确认并保留上传位点。
     * English: Valid current bytes do not mean a logical Block has stopped accepting data; an OPEN Block must reject acknowledgement without advancing upload position.
     * @throws Exception 中文：测试 WAL 创建或资源操作失败；English: test-WAL creation or resource operations fail
     */
    @Test
    public void unsealedBlockCannotBeAcknowledgedEvenWhenItsCurrentBytesAreValid() throws Exception {
        try (WalFixture fixture = fixture(FIRST); MappedFileReader reader = fixture.reader()) {
            assertArrayEquals(FIRST, reader.readAll().toArray(ValueLayout.JAVA_BYTE));
            assertThrows(IllegalStateException.class, reader::ackUpLoadPosition);
            assertEquals(0, fixture.file.upLoadPosition);
            assertFalse(fixture.file.isBlockUploaded(0));
        }
    }

    /**
     * 中文：尾记录全零时原始解析器只能看到有效前缀，Reader 仍须通过 expectedBytes 发现缺失并拒绝返回或确认。
     * English: With a zeroed tail record, the raw decoder sees only a valid prefix; the Reader must detect missing bytes through expectedBytes and refuse output or acknowledgement.
     * @throws Exception 中文：测试 WAL 创建或故障注入失败；English: test-WAL creation or fault injection fails
     */
    @Test
    public void zeroedTailRecordCannotBeMistakenForACompleteSnapshot() throws Exception {
        try (WalFixture fixture = fixture(FIRST, SECOND)) {
            // 元数据登记了两条记录，但尾部第二条全部为零；与中间零洞不同，协议扫描会只得到第一条。
            // 中文：仅清零记录内存，不减少已登记字节计数，模拟预留成功但尚未复制完成的尾部。
            // English: Zero only record memory while retaining registered byte counts, modeling a reserved but not yet copied tail.
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

    /**
     * 中文：只翻转第二条 checksum 的一位，要求整块校验先失败而不是先返回第一条有效数据。
     * English: Flips one bit in the second checksum, requiring whole-Block failure before returning the first valid record.
     * @throws Exception 中文：临时 WAL 创建或损坏注入失败；English: temporary-WAL creation or corruption injection fails
     */
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

    /**
     * 中文：将第二条 magic 改为未知值，检查共用解析器与人工 Reader 一致拒绝损坏块。
     * English: Replaces the second magic with an unknown value and checks consistent rejection by the shared decoder and manual Reader.
     * @throws Exception 中文：测试 WAL 创建或资源操作失败；English: test-WAL creation or resource operations fail
     */
    @Test
    public void illegalMagicRejectsWholeBlock() throws Exception {
        try (WalFixture fixture = fixture(FIRST, SECOND)) {
            fixture.block().set(ValueLayout.JAVA_INT_UNALIGNED, serialized(FIRST), 0x12345678);
            assertRejectedWithoutAcknowledgement(fixture);
        }
    }

    /**
     * 中文：分别注入负数、零、极大值和越界 Value 长度，检查不能借坏长度推进确认位点。
     * English: Injects negative, zero, huge, and out-of-bounds Value lengths, verifying malformed lengths cannot advance acknowledgement.
     * @throws Exception 中文：每轮临时 WAL 创建或故障注入失败；English: per-case temporary-WAL creation or fault injection fails
     */
    @Test
    public void invalidLengthsCannotBeAcknowledged() throws Exception {
        for (int length : new int[]{-1, 0, Integer.MAX_VALUE, BLOCK_SIZE}) {
            try (WalFixture fixture = fixture(FIRST, SECOND)) {
                fixture.block().set(ValueLayout.JAVA_INT_UNALIGNED, serialized(FIRST) + 8, length);
                assertRejectedWithoutAcknowledgement(fixture);
            }
        }
    }

    /**
     * 中文：清零中间记录并保留后一条真实数据，验证零 magic 必须检查余区，不能截断上传有效前缀。
     * English: Zeros a middle record while retaining later valid data, verifying zero magic requires checking the remainder rather than truncating to a valid prefix.
     * @throws Exception 中文：测试 WAL 创建或故障注入失败；English: test-WAL creation or fault injection fails
     */
    @Test
    public void zeroReservationHoleCannotHideAValidLaterRecord() throws Exception {
        try (WalFixture fixture = fixture(FIRST, SECOND, THIRD)) {
            fixture.block().asSlice(serialized(FIRST), serialized(SECOND)).fill((byte) 0);
            assertRejectedWithoutAcknowledgement(fixture);
        }
    }

    /**
     * 中文：空块可以得到零长度只读堆外结果，但不能因此把未提交块确认为已上传。
     * English: An empty block may produce a zero-length read-only native result, but cannot thereby acknowledge an uncommitted block as uploaded.
     * @throws Exception 中文：空 WAL 创建或资源操作失败；English: empty-WAL creation or resource operations fail
     */
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

    /**
     * 中文：校验协议终止边界：不足四字节的剩余空间可结束，而已有 magic 的不完整头必须报错。
     * English: Verifies protocol termination boundaries: fewer than four remaining bytes may end parsing, but a truncated header with magic must fail.
     */
    @Test
    public void sharedDecoderAcceptsLessThanFourTailBytesButRejectsTruncatedHeader() {
        byte[] bytes = new byte[Math.toIntExact(serialized(FIRST)) + 3];
        MemorySegment segment = MemorySegment.ofArray(bytes);
        // WAL 写入器要求四字节对齐；用堆外段构造真实协议，再复制到解码器的堆内输入。
        // 中文：原生段显式四字节对齐，仅用于构造协议；最终解码输入是堆内数组，不能把写入对齐要求强加给 Reader。
        // English: Explicit four-byte native alignment is used only to construct the record; final decoding uses a heap array without imposing writer alignment on the Reader.
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

    /**
     * 中文：对同一损坏块同时检查原始解析和 Reader 全部读取/确认入口，并断言上传位点与逐块标志保持未确认。
     * English: Checks raw decoding and all Reader read/acknowledgement entry points against one corrupt Block, then verifies upload position and block acknowledgement remain unchanged.
     * @param fixture 中文：已完成内存故障注入的 WAL 夹具；English: WAL fixture with corruption already injected
     */
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

    /**
     * 中文：在新临时目录中同步写入指定记录；构造测试数据失败时关闭管理器，成功时由调用方关闭夹具。
     * English: Synchronously writes records in a fresh temporary directory; closes the manager on setup failure and transfers fixture cleanup to the caller on success.
     * @param values 中文：按 WAL 顺序写入的原始 Value 数组；English: original Value arrays in WAL order
     * @return 中文：具有真实映射和元数据的测试夹具；English: fixture with real mappings and metadata
     * @throws Exception 中文：临时目录、WAL 初始化或追加失败；English: temporary-directory, WAL initialization, or append operations fail
     */
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

    /**
     * 中文：独立计算当前 WAL 协议占用长度，用于精确定位下一条头部和注入损坏；不是 Value 输出长度。
     * English: Independently computes serialized WAL occupancy for locating the next header and injecting corruption; this is not the returned Value length.
     * @param value 中文：待计算的原始 Value；English: original Value to measure
     * @return 中文：12 字节头加四字节对齐后的 Value 占用；English: 12-byte header plus four-byte-aligned Value occupancy
     */
    private static long serialized(byte[] value) {
        return DataStruct.HEADER_LENGTH + ((value.length + 3L) & ~3L);
    }

    /**
     * 中文：按输入顺序拼接期望的纯 Value 字节，不添加协议头或填充，用作 readAll 的独立断言基准。
     * English: Concatenates expected Value bytes in order without headers or padding, providing an independent readAll assertion baseline.
     * @param values 中文：需要拼接的测试数组；English: test arrays to concatenate
     * @return 中文：新建的纯 Value 数组；English: newly allocated Value-only array
     */
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

    /**
     * 中文：拥有小型 WAL 管理器的夹具；关闭管理器不关闭已经创建的独立 Reader 快照。
     * English: Fixture owning a small WAL manager; closing it does not close independently created Reader snapshots.
     */
    private static final class WalFixture implements AutoCloseable {
        /**
         * 中文：本用例独立 Bucket 目录，由外层 TemporaryFolder 管理目录清理。
         * English: Isolated Bucket directory whose filesystem cleanup belongs to the outer TemporaryFolder.
         */
        private final Path directory;
        /**
         * 中文：夹具负责关闭的真实 WAL 管理器，其后台线程在构造后立即停止。
         * English: Real WAL manager owned by this fixture, with background threads stopped immediately after construction.
         */
        private final MappedFileManager manager;
        /**
         * 中文：第一个活动 WAL 文件的引用，用于检查位点并修改第零块测试字节。
         * English: Reference to the first active WAL file, used to inspect positions and mutate test bytes in block zero.
         */
        private final DefaultMappedFile file;
        /**
         * 中文：夹具关闭标记，避免主动关闭与 try-with-resources 再次关闭管理器。
         * English: Fixture-close flag preventing repeated manager closure after explicit and try-with-resources cleanup.
         */
        private boolean closed;

        /**
         * 中文：创建两块数据区的映射 WAL 并停止定时任务，使测试只依赖显式封口、读取和确认。
         * English: Creates a two-block mapped WAL and stops timers so tests rely only on explicit sealing, reading, and acknowledgement.
         * @param directory 中文：本用例临时 Bucket 目录；English: temporary Bucket directory for this case
         */
        private WalFixture(Path directory) {
            this.directory = directory;
            BucketConfig config = new BucketConfig().setS3KeyPrefix("manual-reader")
                    .setBlockSize(BLOCK_SIZE).setWalFileSize(2L * BLOCK_SIZE).setCacheSize(2L * BLOCK_SIZE)
                    .setWarmWalFile(false).setLockMappedFilePageCache(false);
            config.chackMappedFileTime = 60000;
            manager = new MappedFileManager(directory.toString(), "reader-test", "bucket", config, 0);
            // 中文：冻结后台调度，不让文件清理或定时刷盘掩盖被测的显式状态转换。
            // English: Freeze background scheduling so cleanup or timed flushing cannot mask the explicit state transitions under test.
            manager.stopAllThread();
            file = manager.getActiveMappedFile().get();
        }

        /**
         * 中文：借用第零块的可写原始 WAL 视图，仅供本测试注入协议损坏；其寿命随源文件映射。
         * English: Borrows a writable raw WAL view of block zero for test corruption injection; its lifetime follows the source mapping.
         * @return 中文：含记录头和填充的第零块映射切片；English: mapped block-zero slice including record headers and padding
         */
        private MemorySegment block() {
            return file.getBlockMappedMemorySegmentSlice(0);
        }

        /**
         * 中文：为第零块构造拥有独立 Arena 的人工恢复 Reader，调用方必须单独关闭。
         * English: Constructs a manual-recovery Reader with its own Arena for block zero; the caller must close it separately.
         * @return 中文：新的只读快照 Reader；English: new read-only snapshot Reader
         */
        private MappedFileReader reader() {
            return new MappedFileReader(file, block(), BLOCK_SIZE,
                    new DeadDataInfo("reader-test", "bucket", file.fileFromOffset, 0, "manual-reader/key"));
        }

        /**
         * 中文：幂等关闭夹具拥有的 WAL 管理器；只有关闭成功才记录标记，允许失败后再次尝试。
         * English: Idempotently closes the owned WAL manager, setting the flag only on success so failures can be retried.
         */
        @Override
        public void close() {
            if (!closed) {
                manager.close();
                closed = true;
            }
        }
    }
}
