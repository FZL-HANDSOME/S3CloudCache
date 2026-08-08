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
public class FileMetaInfoUtil {

    private static final Logger log = LoggerFactory.getLogger(LogName.FILE_META_INFO_UTIL);

    //刷新文件开头4KB元数据区域
    public static void flushFileMetaInfo(DefaultMappedFile mappedFile) {
        try {
            long readPos = mappedFile.readPosition;
            long uploadPos = mappedFile.upLoadPosition;
            // Use file creation time if needed; fall back to current time.
            long updateTime = System.currentTimeMillis();
            // Write into the first 4KB of the mapped file.
            MemorySegment metaSegment = mappedFile.getMappedMemorySegmentSlice(0, FileMetaInfo.FILE_META_SIZE);
            long pos = 0;
            metaSegment.set(ValueLayout.JAVA_LONG, pos, readPos);
            pos += Long.BYTES;
            metaSegment.set(ValueLayout.JAVA_LONG, pos, uploadPos);
            pos += Long.BYTES;
            metaSegment.set(ValueLayout.JAVA_LONG, pos, updateTime);
            // Ensure durability.
            metaSegment.force();
            if (mappedFile.fileFromOffset == readPos && mappedFile.upLoadPosition == uploadPos) {
                DefaultMappedFile.DIRTY_UPDATER.compareAndSet(mappedFile, 1, 0);
            }
        } catch (Exception e) {
            log.warn("flushFileMeta: failed to write meta for {}file offset ", mappedFile.getFileName(), e);
        }
    }

    public static FileMetaInfo getFileMetaInfo(DefaultMappedFile mappedFile) {
        long curPos = 0;
        if (mappedFile == null) {
            return null;
        }
        long readPos = mappedFile.getLong(curPos);
        curPos += 8;
        long updatePos = mappedFile.getLong(curPos);
        curPos += 8;
        long updateTime = mappedFile.getLong(curPos);
        return new FileMetaInfo(readPos, updatePos, updateTime);
    }
}
