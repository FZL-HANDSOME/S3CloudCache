package org.foreverfzl.cloudcache.wal.Util;

import org.foreverfzl.cloudcache.wal.datastruct.FileMetaInfo;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.LogName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * 文件元数据区域工具类
 */
/**
 * 中文：读写WAL文件头0/8/16三个long检查点；偏移64起的上传确认字节由文件对象另行持久化。本工具没有增加头部CRC或事务日志。
 * English: Reads/writes the three checkpoint longs at WAL-header offsets 0/8/16. Upload acknowledgement bytes from offset 64 are persisted separately by the file; this utility adds no header CRC or transaction log.
 */
public class FileMetaInfoUtil {

    /**
     * 中文：文件检查点刷新诊断日志；失败保留dirty以供以后重试。
     * English: Checkpoint-flush diagnostics; failures leave dirty set for a later retry.
     */
    private static final Logger log = LoggerFactory.getLogger(LogName.FILE_META_INFO_UTIL);

    //刷新文件开头4KB元数据区域
    /**
     * 中文：在文件锁内获取位置快照、写头部、force后清除dirty，与位点推进/清理互斥。异常记录日志后返回，不向调用方提供成功结果。
     * English: Under the file monitor, snapshots positions, writes the header, forces, then clears dirty, excluding position advancement/cleanup. Errors are logged and swallowed; no success result is returned.
     *
     * @param mappedFile 中文：生命周期有效的目标文件；English: target file with a valid lifecycle
     */
    public static void flushFileMetaInfo(DefaultMappedFile mappedFile) {
        synchronized (mappedFile) {
            if (mappedFile.isCleanup()) return;
            try {
                long fileFromOffset = mappedFile.fileFromOffset;
                long readPos = mappedFile.readPosition;
                long uploadPos = mappedFile.upLoadPosition;
                long updateTime = System.currentTimeMillis();
                MemorySegment metaSegment = mappedFile.getMappedMemorySegmentSlice(0, FileMetaInfo.FILE_META_SIZE);
                long pos = 0;
                metaSegment.set(ValueLayout.JAVA_LONG, pos, readPos);
                pos += Long.BYTES;
                metaSegment.set(ValueLayout.JAVA_LONG, pos, uploadPos);
                pos += Long.BYTES;
                metaSegment.set(ValueLayout.JAVA_LONG, pos, updateTime);
                // 中文：持有文件锁直到force后清除dirty，避免新的位点变更被旧快照误清零。
                // English: Hold the file monitor until force and dirty clearing complete, preventing a stale snapshot from clearing a newer position update.
                metaSegment.force();
                DefaultMappedFile.DIRTY_UPDATER.set(mappedFile, 0);
                log.info("fileName= {} flushFileMetaInfo successfully, readPos={},uploadPos={},updateTime={}", fileFromOffset, readPos, uploadPos, updateTime);
            } catch (Exception e) {
                log.warn("flushFileMeta: failed to write meta for {}file offset ", mappedFile.getFileName(), e);
            }
        }
    }

    /**
     * 中文：借用文件映射读取三个long，不force、不校验位点，也不获取引用；调用者负责阻止读取期间卸载，restorePositions负责范围校验。
     * English: Reads three longs from the borrowed mapping without forcing, validating positions, or acquiring a reference. Callers prevent unmapping; restorePositions validates ranges.
     *
     * @param mappedFile 中文：待读取文件，可为null；English: file to read, possibly null
     * @return 中文：原始检查点快照，入参null时为null；English: raw checkpoint snapshot, or null for null input
     */
    public static FileMetaInfo getFileMetaInfo(DefaultMappedFile mappedFile) {
        if (mappedFile == null) {
            return null;
        }
        MemorySegment metaSegment = mappedFile.getMappedMemorySegmentSlice(0, FileMetaInfo.FILE_META_SIZE);
        long curPos = 0;
        long readPos = metaSegment.get(ValueLayout.JAVA_LONG, curPos);
        curPos += 8;
        long updatePos = metaSegment.get(ValueLayout.JAVA_LONG, curPos);
        curPos += 8;
        long updateTime = metaSegment.get(ValueLayout.JAVA_LONG, curPos);
        return new FileMetaInfo(readPos, updatePos, updateTime);
    }
}
