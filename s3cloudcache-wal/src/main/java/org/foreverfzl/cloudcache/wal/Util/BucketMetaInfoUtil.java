package org.foreverfzl.cloudcache.wal.Util;

import org.foreverfzl.cloudcache.wal.datastruct.BucketMetaInfo;
import org.foreverfzl.cloudchache.common.exception.WalException;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.stream.Stream;

/** Bucket 元数据读写；损坏与不存在必须区分，不能静默跳过需要恢复的 WAL。 */
public class BucketMetaInfoUtil {
    private static final String META_FILE_NAME = "bucketMeta";
    private static final int META_FILE_SIZE = 4096;
    // 仍使用原来的 24 字节固定头，不占用前缀空间。最低位为 dirty，其余位标识 V2。
    // 旧文件首 int 只有 0/1，仍按旧 CRC 算法读取；新文件不允许退回旧算法校验。
    private static final int FORMAT_V2 = 0x424D0200;

    /** 已有相同配置只重新映射，绝不截断；新建或无遗留 WAL 的配置变更才写 V2。 */
    public static MemorySegment createAndMapBucketMetaFile(BucketMetaInfo metadata, Path directory, Arena arena) {
        Path metaPath = directory.resolve(META_FILE_NAME);
        try {
            Files.createDirectories(directory);
            BucketMetaInfo existing = readBucketMetaFile(directory);
            boolean sameConfiguration = existing != null
                    && existing.getBlockSize() == metadata.getBlockSize()
                    && existing.getFileSize() == metadata.getFileSize()
                    && Arrays.equals(existing.getData(), metadata.getData());
            if (!sameConfiguration) {
                if (hasWalFiles(directory)) {
                    throw new WalException("Cannot change bucket metadata while WAL files remain: " + directory);
                }
                writeMetadataAtomically(metadata, directory);
            }
            try (FileChannel channel = FileChannel.open(metaPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                MemorySegment segment = channel.map(FileChannel.MapMode.READ_WRITE, 0, META_FILE_SIZE, arena);
                // 映射旧文件时保持原版本与 CRC，只把 dirty 置为 1；不能先升级再恢复旧 WAL。
                updateIsDirty(1, segment);
                return segment;
            }
        } catch (IOException e) {
            throw new WalException("Cannot create/map bucket metadata: " + metaPath, e);
        }
    }

    private static void writeMetadataAtomically(BucketMetaInfo metadata, Path directory) throws IOException {
        byte[] prefix = metadata.getData();
        ByteBuffer bytes = ByteBuffer.allocate(META_FILE_SIZE).order(ByteOrder.nativeOrder());
        bytes.putInt(FORMAT_V2 | metadata.getIsDirty());
        bytes.putInt(metadata.getBlockSize());
        bytes.putLong(metadata.getFileSize());
        bytes.putInt(metadata.getCrc());
        bytes.putInt(prefix.length);
        bytes.put(prefix);
        bytes.position(0);
        Path temporary = Files.createTempFile(directory, ".bucketMeta-", ".tmp");
        try {
            // 完整写入并强制落盘临时文件后才替换原件。原子替换不受支持时直接失败，不截断原件。
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                while (bytes.hasRemaining()) channel.write(bytes);
                channel.force(true);
            }
            Files.move(temporary, directory.resolve(META_FILE_NAME),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    /** 仅在元数据确实不存在且没有 WAL 时返回 null；任何损坏都拒绝继续恢复/覆盖。 */
    public static BucketMetaInfo readBucketMetaFile(Path directory) {
        Path metaPath = directory.resolve(META_FILE_NAME);
        try {
            if (!Files.exists(metaPath)) {
                if (hasWalFiles(directory)) throw new WalException("WAL exists without bucket metadata: " + directory);
                return null;
            }
            if (!Files.isRegularFile(metaPath) || Files.size(metaPath) != META_FILE_SIZE) {
                throw new WalException("Invalid bucket metadata file size: " + metaPath);
            }
            byte[] bytes = Files.readAllBytes(metaPath);
            if (bytes.length != META_FILE_SIZE) throw new WalException("Truncated bucket metadata: " + metaPath);
            ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
            int flags = buffer.getInt();
            boolean legacy = flags == 0 || flags == 1;
            if (!legacy && (flags & ~1) != FORMAT_V2) {
                throw new WalException("Unknown bucket metadata version: " + metaPath);
            }
            int blockSize = buffer.getInt();
            long fileSize = buffer.getLong();
            int storedCrc = buffer.getInt();
            int prefixLength = buffer.getInt();
            if (prefixLength < 0 || prefixLength > BucketMetaInfo.MAX_PREFIX_BYTES) {
                throw new WalException("Invalid bucket metadata prefix length: " + metaPath);
            }
            byte[] prefix = new byte[prefixLength];
            buffer.get(prefix);
            BucketMetaInfo metadata;
            try {
                metadata = new BucketMetaInfo(flags & 1, blockSize, fileSize, prefix);
            } catch (IllegalArgumentException e) {
                throw new WalException("Invalid bucket metadata fields: " + metaPath, e);
            }
            int expectedCrc = legacy ? metadata.legacyCrc() : metadata.getCrc();
            if (storedCrc != expectedCrc) throw new WalException("Bucket metadata CRC mismatch: " + metaPath);
            return metadata;
        } catch (IOException e) {
            throw new WalException("Cannot read bucket metadata: " + metaPath, e);
        }
    }

    private static boolean hasWalFiles(Path directory) throws IOException {
        Path walDirectory = directory.resolve("wal");
        if (!Files.exists(walDirectory)) return false;
        if (!Files.isDirectory(walDirectory)) throw new WalException("WAL path is not a directory: " + walDirectory);
        try (Stream<Path> paths = Files.list(walDirectory)) {
            return paths.anyMatch(Files::isRegularFile);
        }
    }

    /** dirty 是可变标记，不参与配置 CRC；修改时必须保留 V2 标记，防止误按旧 CRC 解释。 */
    public static void updateIsDirty(int isDirty, MemorySegment segment) {
        if (isDirty != 0 && isDirty != 1) throw new IllegalArgumentException("isDirty must be 0 or 1");
        int flags = segment.get(ValueLayout.JAVA_INT, 0);
        if (flags == 0 || flags == 1) {
            segment.set(ValueLayout.JAVA_INT, 0, isDirty);
        } else if ((flags & ~1) == FORMAT_V2) {
            segment.set(ValueLayout.JAVA_INT, 0, FORMAT_V2 | isDirty);
        } else {
            throw new WalException("Cannot update unknown bucket metadata version");
        }
        segment.force();
    }
}
