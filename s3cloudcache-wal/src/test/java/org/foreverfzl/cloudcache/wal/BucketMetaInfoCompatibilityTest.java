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
public class BucketMetaInfoCompatibilityTest {
    private static final int BLOCK_SIZE = 2 * 1024 * 1024;
    private static final long FILE_SIZE = 256L * 1024 * 1024;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Path directory(String name) throws Exception {
        return temporaryFolder.newFolder(name).toPath();
    }

    private void create(Path directory, BucketMetaInfo metadata) {
        try (Arena arena = Arena.ofShared()) {
            BucketMetaInfoUtil.createAndMapBucketMetaFile(metadata, directory, arena);
        }
    }

    private BucketMetaInfo metadata(String prefix) {
        return new BucketMetaInfo(BLOCK_SIZE, FILE_SIZE, prefix);
    }

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

    @Test
    public void fullFieldCrcDistinguishesConfigurationsThatLegacyCrcMissed() {
        BucketMetaInfo first = metadata("same");
        BucketMetaInfo second = new BucketMetaInfo(2 * BLOCK_SIZE, 2 * FILE_SIZE, "same");
        assertEquals("证明历史低 8 位算法漏检这些字段变化", first.legacyCrc(), second.legacyCrc());
        assertNotEquals(first.getCrc(), second.getCrc());
    }

    @Test
    public void legacyMetadataCanBeReadAndReopenedWithoutTruncationOrUpgrade() throws Exception {
        Path directory = directory("legacy");
        byte[] original = legacyBytes(BLOCK_SIZE, FILE_SIZE, "历史前缀");
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
