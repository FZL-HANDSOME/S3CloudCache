package org.foreverfzl.cloudcache.wal;

import org.foreverfzl.cloudcache.wal.Util.BucketMetaInfoUtil;
import org.foreverfzl.cloudcache.wal.datastruct.BucketMetaInfo;
import org.foreverfzl.cloudchache.common.exception.WalException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;

import static org.junit.Assert.*;

/** 只操作临时目录中的 4 KiB 元数据，不分配 2 GiB WAL，也不访问 S3。 */
/**
 * 中文：独立验证bucketMeta新旧协议及失败保护，所有文件由TemporaryFolder隔离；2GiB只作为long字段，不创建2GiB WAL或访问S3。
 * English: Independently tests old/new bucketMeta formats and fail-closed behavior in TemporaryFolder isolation; 2GiB is only a long field, with no 2GiB WAL creation or S3 access.
 */
public class BucketMetaInfoCompatibilityTest {
    /**
     * 中文：测试配置中的2MiB逻辑块大小，不分配同等业务内存。
     * English: 2MiB logical block size in test configuration, without allocating equivalent business memory.
     */
    private static final int BLOCK_SIZE = 2 * 1024 * 1024;
    /**
     * 中文：测试配置中的256MiB数据区容量，仅写入元数据字段。
     * English: 256MiB data-area capacity stored only as a metadata field.
     */
    private static final long FILE_SIZE = 256L * 1024 * 1024;

    /**
     * 中文：JUnit每例隔离目录，保护真实数据路径。
     * English: Per-test JUnit directory isolation protecting real data paths.
     */
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    /**
     * 中文：在本例TemporaryFolder下创建子目录。
     * English: Creates a child directory under this test's TemporaryFolder.
     *
     * @param name 中文：本例目录名；English: directory name within this test
     * @return 中文：独立临时路径；English: isolated temporary path
     * @throws Exception 中文：目录创建失败；English: directory creation fails
     */
    private Path directory(String name) throws Exception {
        return temporaryFolder.newFolder(name).toPath();
    }

    /**
     * 中文：在短生命周期共享Arena中创建/打开元数据，退出时关闭映射，便于后续字节级故障注入。
     * English: Creates/opens metadata in a short-lived shared arena and unmaps on return, enabling later byte-level fault injection.
     *
     * @param directory 中文：独立临时Bucket目录；English: isolated temporary bucket directory
     * @param metadata 中文：待持久化配置；English: configuration to persist
     */
    private void create(Path directory, BucketMetaInfo metadata) {
        try (Arena arena = Arena.ofShared()) {
            BucketMetaInfoUtil.createAndMapBucketMetaFile(metadata, directory, arena);
        }
    }

    /**
     * 中文：生成固定有效大小的配置，仅改变前缀。
     * English: Builds configuration with fixed valid sizes and a variable prefix.
     *
     * @param prefix 中文：测试前缀；English: test prefix
     * @return 中文：新的配置值对象；English: new configuration value
     */
    private BucketMetaInfo metadata(String prefix) {
        return new BucketMetaInfo(BLOCK_SIZE, FILE_SIZE, prefix);
    }

    /**
     * 中文：独立重现旧CRC和本机字节序布局，不调用生产legacyCrc，以防实现和测试共享同一错误。
     * English: Independently reproduces legacy CRC and native-order layout rather than calling production legacyCrc, avoiding shared implementation/test mistakes.
     *
     * @param blockSize 中文：旧块大小，字节；English: legacy block bytes
     * @param fileSize 中文：旧数据区容量，字节；English: legacy data-area bytes
     * @param prefix 中文：UTF-8前缀；English: UTF-8 prefix
     * @return 中文：4096字节旧格式文件内容；English: 4096-byte legacy file contents
     */
    private byte[] legacyBytes(int blockSize, long fileSize, String prefix) {
        byte[] data = prefix.getBytes(StandardCharsets.UTF_8);
        CRC32 oldCrc = new CRC32();
        // 故意复现历史算法，不调用生产代码的兼容方法，避免测试和实现犯同一个错误。
        oldCrc.update(blockSize);
        oldCrc.update((int) fileSize);
        oldCrc.update(data.length);
        oldCrc.update(data);
        return ByteBuffer.allocate(4096).order(ByteOrder.nativeOrder()).putInt(1)
                .putInt(blockSize).putLong(fileSize).putInt((int) oldCrc.getValue())
                .putInt(data.length).put(data).array();
    }

    /**
     * 中文：断言2GiB字段不会int溢出，dirty从1到0再到1时版本tag及long容量保留；只映射4KiB元数据。
     * English: Asserts the 2GiB field does not overflow int and toggling dirty 1-to-0-to-1 preserves the version tag and long capacity; maps only 4KiB of metadata.
     *
     * @throws Exception 中文：临时文件或映射操作失败；English: temporary-file or mapping operation fails
     */
    @Test
    public void v2SupportsTwoGiBAndPreservesVersionAcrossDirtyChanges() throws Exception {
        Path directory = directory("two-gib");
        BucketMetaInfo metadata = new BucketMetaInfo(BLOCK_SIZE, 2L * 1024 * 1024 * 1024, "两G前缀");
        try (Arena arena = Arena.ofShared()) {
            MemorySegment segment = BucketMetaInfoUtil.createAndMapBucketMetaFile(metadata, directory, arena);
            int flags = segment.get(ValueLayout.JAVA_INT, 0);
            assertNotEquals("新文件必须有独立版本标记", 1, flags);
            BucketMetaInfoUtil.updateIsDirty(0, segment);
            assertEquals(flags & ~1, segment.get(ValueLayout.JAVA_INT, 0));
            BucketMetaInfo clean = BucketMetaInfoUtil.readBucketMetaFile(directory);
            assertEquals(0, clean.getIsDirty());
            assertEquals(2L * 1024 * 1024 * 1024, clean.getFileSize());
            BucketMetaInfoUtil.updateIsDirty(1, segment);
            assertEquals(flags, segment.get(ValueLayout.JAVA_INT, 0));
            assertEquals(1, BucketMetaInfoUtil.readBucketMetaFile(directory).getIsDirty());
        }
    }

    /**
     * 中文：使用两组低字节相同的有效尺寸，先证明旧CRC碰撞，再断言V2 CRC能区分这些具体输入；不宣称CRC无碰撞。
     * English: Uses valid sizes sharing low bytes to demonstrate a legacy CRC collision, then checks V2 distinguishes these specific inputs; does not claim CRC is collision-free.
     */
    @Test
    public void fullFieldCrcDistinguishesConfigurationsThatLegacyCrcMissed() {
        BucketMetaInfo first = metadata("same");
        BucketMetaInfo second = new BucketMetaInfo(2 * BLOCK_SIZE, 2 * FILE_SIZE, "same");
        assertEquals("证明历史低 8 位算法漏检这些字段变化", first.legacyCrc(), second.legacyCrc());
        assertNotEquals(first.getCrc(), second.getCrc());
    }

    /**
     * 中文：构造旧格式及WAL占位文件，在页尾加入哨兵；重开同配置后逐字节比较，证明未截断或升级旧页。
     * English: Builds legacy metadata and a WAL placeholder with a trailing sentinel; byte-for-byte comparison after reopening the same configuration proves the page was neither truncated nor upgraded.
     *
     * @throws Exception 中文：临时文件或映射操作失败；English: temporary-file or mapping operation fails
     */
    @Test
    public void legacyMetadataCanBeReadAndReopenedWithoutTruncationOrUpgrade() throws Exception {
        Path directory = directory("legacy");
        byte[] original = legacyBytes(BLOCK_SIZE, FILE_SIZE, "历史前缀");
        // 中文：页尾哨兵位于前缀之外；若实现截断重建文件，逐字节断言将发现哨兵丢失。
        // English: The trailing sentinel lies outside the prefix; byte comparison detects its loss if the implementation truncates and rebuilds the file.
        original[4095] = 0x5a;
        Files.write(directory.resolve("bucketMeta"), original);
        Files.createDirectories(directory.resolve("wal"));
        Files.write(directory.resolve("wal/0"), new byte[]{1, 2, 3});
        BucketMetaInfo loaded = BucketMetaInfoUtil.readBucketMetaFile(directory);
        assertEquals(BLOCK_SIZE, loaded.getBlockSize());
        assertEquals(FILE_SIZE, loaded.getFileSize());
        assertEquals("历史前缀", new String(loaded.getData(), StandardCharsets.UTF_8));
        create(directory, metadata("历史前缀"));
        assertArrayEquals("存在 WAL 时仅重新映射，不截断也不升级旧格式", original,
                Files.readAllBytes(directory.resolve("bucketMeta")));
    }

    /**
     * 中文：分别修改Block与文件容量的高位但保留原CRC，断言V2读取失败，不能降级为旧低8位校验。
     * English: Changes high bits of block/file capacities while retaining CRC and asserts V2 reading fails instead of downgrading to legacy low-byte validation.
     *
     * @throws Exception 中文：临时文件或映射操作失败；English: temporary-file or mapping operation fails
     */
    @Test
    public void v2HighBitFieldCorruptionDoesNotFallBackToLegacyCrc() throws Exception {
        Path directory = directory("high-bits");
        create(directory, metadata("prefix"));
        byte[] original = Files.readAllBytes(directory.resolve("bucketMeta"));
        byte[] corrupted = original.clone();
        ByteBuffer.wrap(corrupted).order(ByteOrder.nativeOrder()).putInt(4, 2 * BLOCK_SIZE);
        Files.write(directory.resolve("bucketMeta"), corrupted);
        assertThrows(WalException.class, () -> BucketMetaInfoUtil.readBucketMetaFile(directory));
        corrupted = original.clone();
        ByteBuffer.wrap(corrupted).order(ByteOrder.nativeOrder()).putLong(8, 2 * FILE_SIZE);
        Files.write(directory.resolve("bucketMeta"), corrupted);
        assertThrows(WalException.class, () -> BucketMetaInfoUtil.readBucketMetaFile(directory));
    }

    /**
     * 中文：即使dirty为0，只要磁盘还有WAL就拒绝不同前缀，并检查元数据与WAL字节都未被修改。
     * English: Even with dirty zero, existing WAL must block a changed prefix; verifies metadata and WAL bytes remain unchanged.
     *
     * @throws Exception 中文：临时文件或映射操作失败；English: temporary-file or mapping operation fails
     */
    @Test
    public void walPreventsConfigurationReplacementEvenWithCleanFlag() throws Exception {
        Path directory = directory("config-conflict");
        try (Arena arena = Arena.ofShared()) {
            MemorySegment segment = BucketMetaInfoUtil.createAndMapBucketMetaFile(metadata("old"), directory, arena);
            BucketMetaInfoUtil.updateIsDirty(0, segment);
        }
        Files.createDirectories(directory.resolve("wal"));
        Files.write(directory.resolve("wal/0"), new byte[]{7, 8, 9});
        byte[] original = Files.readAllBytes(directory.resolve("bucketMeta"));
        assertThrows(WalException.class, () -> create(directory, metadata("new")));
        assertArrayEquals(original, Files.readAllBytes(directory.resolve("bucketMeta")));
        assertArrayEquals(new byte[]{7, 8, 9}, Files.readAllBytes(directory.resolve("wal/0")));
    }

    /**
     * 中文：没有遗留WAL时替换完整配置，读取新值并检查4KiB大小与无残留临时文件；未模拟原子移动失败。
     * English: Replaces configuration with no WAL remaining, verifies new values, 4KiB size, and no leftover temporary file; does not simulate atomic-move failure.
     *
     * @throws Exception 中文：临时文件或映射操作失败；English: temporary-file or mapping operation fails
     */
    @Test
    public void configurationCanBeReplacedWhenNoWalRemains() throws Exception {
        Path directory = directory("config-change");
        create(directory, metadata("old"));
        create(directory, new BucketMetaInfo(2 * BLOCK_SIZE, 2 * FILE_SIZE, "new"));
        BucketMetaInfo current = BucketMetaInfoUtil.readBucketMetaFile(directory);
        assertEquals(2 * BLOCK_SIZE, current.getBlockSize());
        assertEquals(2 * FILE_SIZE, current.getFileSize());
        assertEquals("new", new String(current.getData(), StandardCharsets.UTF_8));
        assertEquals(4096, Files.size(directory.resolve("bucketMeta")));
        try (var files = Files.list(directory)) {
            assertFalse("原子替换完成后不残留临时文件", files.anyMatch(path -> path.getFileName().toString().endsWith(".tmp")));
        }
    }

    /**
     * 中文：先验证真正空目录返回null，再加入WAL占位，断言缺失元数据不能被静默重建覆盖。
     * English: First verifies null for a truly empty directory, then adds a WAL placeholder and asserts missing metadata cannot be silently recreated.
     *
     * @throws Exception 中文：临时文件或映射操作失败；English: temporary-file or mapping operation fails
     */
    @Test
    public void missingMetadataIsOnlyEmptyWhenWalIsAlsoAbsent() throws Exception {
        Path directory = directory("missing");
        assertNull(BucketMetaInfoUtil.readBucketMetaFile(directory));
        Files.createDirectories(directory.resolve("wal"));
        Files.write(directory.resolve("wal/0"), new byte[]{1});
        assertThrows(WalException.class, () -> BucketMetaInfoUtil.readBucketMetaFile(directory));
        assertThrows(WalException.class, () -> create(directory, metadata("prefix")));
        assertFalse(Files.exists(directory.resolve("bucketMeta")));
    }

    /**
     * 中文：注入负/超大前缀长度、未知版本与23字节短文件，读取和重新创建均应拒绝；原短文件保留。
     * English: Injects negative/huge prefix lengths, unknown version, and a 23-byte truncated file; reads/recreation must reject and retain the short original.
     *
     * @throws Exception 中文：临时文件或映射操作失败；English: temporary-file or mapping operation fails
     */
    @Test
    public void invalidLengthVersionAndTruncatedMetadataFailClosed() throws Exception {
        Path directory = directory("corruption");
        create(directory, metadata("prefix"));
        byte[] original = Files.readAllBytes(directory.resolve("bucketMeta"));
        for (int invalidLength : new int[]{-1, Integer.MAX_VALUE}) {
            byte[] bytes = original.clone();
            ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()).putInt(20, invalidLength);
            Files.write(directory.resolve("bucketMeta"), bytes);
            assertThrows(WalException.class, () -> BucketMetaInfoUtil.readBucketMetaFile(directory));
        }
        byte[] bytes = original.clone();
        ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()).putInt(0, 0x12345678);
        Files.write(directory.resolve("bucketMeta"), bytes);
        assertThrows(WalException.class, () -> BucketMetaInfoUtil.readBucketMetaFile(directory));
        Files.write(directory.resolve("bucketMeta"), new byte[23]);
        assertThrows(WalException.class, () -> BucketMetaInfoUtil.readBucketMetaFile(directory));
        assertThrows(WalException.class, () -> create(directory, metadata("prefix")));
        assertEquals(23, Files.size(directory.resolve("bucketMeta")));
    }

    /**
     * 中文：覆盖4072字节合法前缀、多字节UTF-8越界及null；非法参数不得改变已写元数据。
     * English: Covers a valid 4072-byte prefix, multi-byte UTF-8 overflow, and null; invalid parameters must not alter existing metadata.
     *
     * @throws Exception 中文：临时文件或映射操作失败；English: temporary-file or mapping operation fails
     */
    @Test
    public void prefixLimitIsMeasuredInUtf8BytesBeforeTouchingFiles() throws Exception {
        Path directory = directory("prefix-boundary");
        String maximum = "a".repeat(BucketMetaInfo.MAX_PREFIX_BYTES);
        create(directory, metadata(maximum));
        assertEquals(BucketMetaInfo.MAX_PREFIX_BYTES, BucketMetaInfoUtil.readBucketMetaFile(directory).getDataLen());
        assertThrows(IllegalArgumentException.class, () -> metadata("字".repeat(1358)));
        assertThrows(IllegalArgumentException.class, () -> metadata(null));
        assertEquals(maximum, new String(BucketMetaInfoUtil.readBucketMetaFile(directory).getData(), StandardCharsets.UTF_8));
    }
}
