package org.foreverfzl.cloudcache.wal.Util;

import org.foreverfzl.cloudcache.wal.datastruct.BucketMetaInfo;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.exception.WalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * bucket元数据文件工具类
 */
public class BucketMetaInfoUtil {
    private static final Logger log = LoggerFactory.getLogger(LogName.BUCKET_META_INFO_UTIL);

    // Bucket元数据文件名字
    private static final String META_FILE_NAME = "bucketMeta";

    // bucket元数据文件大小4KB
    private static final int META_FILE_SIZE = 4 * 1024;


    /**
     * 由外部传入生命的外部 Arena，并将创建好的 MemorySegment 作为返回值返回
     * dirPath指的是具体文件的上级目录
     */
    public static MemorySegment createAndMapBucketMetaFile(BucketMetaInfo bucketMetaInfo, Path dirPath, Arena externalArena) {
        Path metaPath = dirPath.resolve(META_FILE_NAME);
        try {
            if (metaPath.getParent() != null) {
                Files.createDirectories(metaPath.getParent());
            }
            // FileChannel 在完成 mmap 后即可安全关闭，映射区域生命周期由 externalArena 绑定
            try (FileChannel fileChannel = FileChannel.open(
                    metaPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {

                MemorySegment segment = fileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0,
                        META_FILE_SIZE,
                        externalArena
                );
                long pos = 0;
                segment.set(ValueLayout.JAVA_INT, pos, bucketMetaInfo.getIsDirty());
                pos += 4;
                segment.set(ValueLayout.JAVA_INT, pos, bucketMetaInfo.getBlockSize());
                pos += 4;
                segment.set(ValueLayout.JAVA_LONG, pos, bucketMetaInfo.getFileSize());
                pos += 8;
                segment.set(ValueLayout.JAVA_INT, pos, bucketMetaInfo.getCrc());
                pos += 4;
                segment.set(ValueLayout.JAVA_INT, pos, bucketMetaInfo.getDataLen());
                pos += 4;
                MemorySegment.copy(
                        bucketMetaInfo.getData(),
                        0,
                        segment,
                        ValueLayout.JAVA_BYTE,
                        pos,
                        bucketMetaInfo.getDataLen()
                );
                segment.force();
                return segment;
            }
        } catch (Exception e) {
            throw new WalException("Create bucketMetaFile failed: " + metaPath, e);
        }
    }

    public static BucketMetaInfo readBucketMetaFile(Path dirPath) {
        int isDirty = 0;
        int oldblockSize = 0;
        long oldFileSize = 0;
        byte[] data = null;
        try {
            Path bucketMetaPath = dirPath.resolve(META_FILE_NAME);
            if (!Files.exists(bucketMetaPath) || !Files.isRegularFile(bucketMetaPath)) {
                return null;
            }
            // 仅读取文件前 4KB (4096 字节)
            byte[] metaBytes;
            try (InputStream in = Files.newInputStream(bucketMetaPath)) {
                metaBytes = in.readNBytes(META_FILE_SIZE);
            }

            MemorySegment segment = MemorySegment.ofArray(metaBytes);
            long pos = 0;

            // 1. 读取 isDirty (使用 JAVA_INT_UNALIGNED)
            isDirty = segment.get(ValueLayout.JAVA_INT_UNALIGNED, pos);
            pos += 4;

            // 2. 读取 blockSize (使用 JAVA_INT_UNALIGNED)
            oldblockSize = segment.get(ValueLayout.JAVA_INT_UNALIGNED, pos);
            pos += 4;

            // 3. 读取 fileSize (使用 JAVA_LONG_UNALIGNED)
            oldFileSize = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, pos);
            pos += 8;

            // 4. 读取 CRC (使用 JAVA_INT_UNALIGNED)
            int oldCrc = segment.get(ValueLayout.JAVA_INT_UNALIGNED, pos);
            pos += 4;

            // 5. 读取 dataLen (使用 JAVA_INT_UNALIGNED)
            int dataLen = segment.get(ValueLayout.JAVA_INT_UNALIGNED, pos);
            pos += 4;

            // 6. 读取 data (byte[])
            data = new byte[dataLen];
            MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, pos, data, 0, dataLen);
            // 校验数据 CRC
            CRC32 crc32 = new CRC32();
            crc32.update(oldblockSize);
            crc32.update(Math.toIntExact(oldFileSize));
            crc32.update(dataLen);
            crc32.update(data);
            // bucketMeta 文件损坏，以前的文件无法恢复
            if ((int) crc32.getValue() != oldCrc) {
                log.warn("An error occurred in the bucketMeta data.old file can not recover. file path is {}", bucketMetaPath);
                return null;
            }
        } catch (Exception e) {
            log.error("readBucketMetaFile failed, path is {} ", dirPath, e);
            return null;
        }
        return new BucketMetaInfo(isDirty, oldblockSize, oldFileSize, data);
    }

    //修改bucketMetaInfo的isDirty
    public static void updateIsDirty(int isDirty, MemorySegment memorySegment) {
        memorySegment.set(ValueLayout.JAVA_INT, 0, isDirty);
        memorySegment.force();
    }

}
