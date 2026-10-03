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
/**
 * 中文：管理独立bucketMeta页，不是WAL记录头。V2复用原24字节固定头，保留旧格式读取；损坏时拒绝继续，不把异常当作空配置。
 * English: Manages the standalone bucketMeta page, not WAL record headers. V2 reuses the original 24-byte fixed header and retains legacy reads; corruption fails closed instead of being treated as empty configuration.
 */
public class BucketMetaInfoUtil {
    /**
     * 中文：Bucket目录下配置文件名，不在wal子目录中。
     * English: Configuration filename in the bucket directory, outside its wal subdirectory.
     */
    private static final String META_FILE_NAME = "bucketMeta";
    /**
     * 中文：固定元数据页大小，单位字节；读取要求文件恰好为4096字节。
     * English: Fixed metadata-page size in bytes; reads require exactly 4096 bytes.
     */
    private static final int META_FILE_SIZE = 4096;
    // 仍使用原来的 24 字节固定头，不占用前缀空间。最低位为 dirty，其余位标识 V2。
    // 旧文件首 int 只有 0/1，仍按旧 CRC 算法读取；新文件不允许退回旧算法校验。
    /**
     * 中文：首int的版本部分；最低位dirty独立变化。旧格式首int只有0/1，未知tag不能降级解释。
     * English: Version portion of the first int; dirty changes independently in bit zero. Legacy first ints are only 0/1; unknown tags must not be downgraded.
     */
    private static final int FORMAT_V2 = 0x424D0200;

    /** 已有相同配置只重新映射，绝不截断；新建或无遗留 WAL 的配置变更才写 V2。 */
    /**
     * 中文：先校验原文件及配置；相同配置只重映射并置dirty=1，有WAL时拒绝改配置。Arena由调用方拥有，返回段在Arena关闭后失效。
     * English: Validates existing metadata/configuration first; matching configuration is remapped with dirty=1, while configuration changes are rejected with WAL present. The caller owns the arena and the returned segment expires with it.
     *
     * @param metadata 中文：已校验的新配置；English: validated requested configuration
     * @param directory 中文：Bucket目录；English: bucket directory
     * @param arena 中文：调用者管理的映射生命周期；English: caller-managed mapping lifetime
     * @return 中文：可写4KiB元数据映射；English: writable 4KiB metadata mapping
     * @throws WalException 中文：旧数据无效、配置冲突或文件操作失败；English: invalid existing data, configuration conflict, or file-operation failure
     */
    public static MemorySegment createAndMapBucketMetaFile(BucketMetaInfo metadata, Path directory, Arena arena) {
        Path metaPath = directory.resolve(META_FILE_NAME);
        try {
            Files.createDirectories(directory);
            BucketMetaInfo existing = readBucketMetaFile(directory);
            boolean sameConfiguration = existing != null
                    && existing.getBlockSize() == metadata.getBlockSize()
                    && existing.getFileSize() == metadata.getFileSize()
                    && Arrays.equals(existing.getData(), metadata.getData());
            // 中文：存在WAL时格式参数不可变；先拒绝再写文件，避免二次启动用新布局解释旧数据。
            // English: Layout parameters cannot change with WAL present; reject before writing so a second restart cannot interpret old data with a new layout.
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

    /**
     * 中文：按本机字节序写V2固定字段和完整前缀到同目录临时文件，force后原子替换；不支持原子移动就失败，不做截断回退。目录项耐久性仍受平台语义约束。
     * English: Writes V2 fields in native order and the full prefix to a same-directory temporary file, forces it, then atomically replaces the target. Unsupported atomic moves fail without truncation fallback; directory-entry durability remains platform-dependent.
     *
     * @param metadata 中文：待保存的配置值对象；English: configuration value to persist
     * @param directory 中文：原件和临时文件所在目录；English: directory holding the target and temporary file
     * @throws IOException 中文：写入、force、移动或临时文件清理失败；English: write, force, move, or temporary-file cleanup fails
     */
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
            // 中文：与编号sidecar的原位更新不同，这里使用已force完整临时页的原子替换，不提供非原子回退。
            // English: Unlike the in-place sequence sidecar, this atomically replaces from a complete forced temporary page with no non-atomic fallback.
            Files.move(temporary, directory.resolve(META_FILE_NAME),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    /** 仅在元数据确实不存在且没有 WAL 时返回 null；任何损坏都拒绝继续恢复/覆盖。 */
    /**
     * 中文：读取并验证完整4KiB页、版本、布局、前缀长度和对应版本CRC；只在没有元数据且没有WAL时返回null。不会升级旧文件或修改dirty。
     * English: Reads and validates the complete 4KiB page, version, layout, prefix length, and version-specific CRC. Returns null only when both metadata and WAL are absent; it neither upgrades legacy files nor changes dirty.
     *
     * @param directory 中文：Bucket目录；English: bucket directory
     * @return 中文：已验证配置；bucketMeta 缺失且未发现 WAL 普通文件时为 null，不要求整个目录为空；English: validated configuration, or null when bucketMeta is absent and no regular WAL files are found; the whole directory need not be empty
     * @throws WalException 中文：元数据缺失但WAL存在，或格式/CRC/I/O异常；English: missing metadata with WAL present, or format/CRC/I/O failure
     */
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
            // 中文：版本先决定CRC算法；V2校验失败不能再尝试弱旧CRC，否则会掩盖字段破坏。
            // English: The version selects the CRC algorithm first; do not retry weak legacy CRC after a V2 mismatch, which could hide field corruption.
            int expectedCrc = legacy ? metadata.legacyCrc() : metadata.getCrc();
            if (storedCrc != expectedCrc) throw new WalException("Bucket metadata CRC mismatch: " + metaPath);
            return metadata;
        } catch (IOException e) {
            throw new WalException("Cannot read bucket metadata: " + metaPath, e);
        }
    }

    /**
     * 中文：只判断wal目录是否含普通文件，不解码记录，也不证明这些文件含未提交数据。
     * English: Checks only for regular files in wal; it does not decode records or prove they contain uncommitted data.
     *
     * @param directory 中文：Bucket目录；English: bucket directory
     * @return 中文：是否至少有一个普通WAL目录文件；English: whether at least one regular file exists in wal
     * @throws IOException 中文：无法列举目录；English: directory listing fails
     */
    private static boolean hasWalFiles(Path directory) throws IOException {
        Path walDirectory = directory.resolve("wal");
        if (!Files.exists(walDirectory)) return false;
        if (!Files.isDirectory(walDirectory)) throw new WalException("WAL path is not a directory: " + walDirectory);
        try (Stream<Path> paths = Files.list(walDirectory)) {
            return paths.anyMatch(Files::isRegularFile);
        }
    }

    /** dirty 是可变标记，不参与配置 CRC；修改时必须保留 V2 标记，防止误按旧 CRC 解释。 */
    /**
     * 中文：只更改0/1恢复提示并force映射页，保留V2版本位及原配置CRC；调用方需协调并发写入和Arena关闭。
     * English: Changes only the 0/1 recovery hint and forces the mapped page, retaining V2 version bits and configuration CRC; callers must coordinate concurrent writes and arena closure.
     *
     * @param isDirty 中文：0或1；English: 0 or 1
     * @param segment 中文：仍有效的bucketMeta可写映射；English: live writable bucketMeta mapping
     * @throws IllegalArgumentException 中文：dirty不是0或1；English: dirty is neither 0 nor 1
     * @throws WalException 中文：首int不是已知格式；English: first int is not a recognized format
     */
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
