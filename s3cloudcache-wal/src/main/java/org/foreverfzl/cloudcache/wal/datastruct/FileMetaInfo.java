package org.foreverfzl.cloudcache.wal.datastruct;

/**
 * 每个文件元数据区域的格式，该类仅供参考
 */
public class FileMetaInfo {

    public static final long FILE_META_SIZE = 4 * 1024;

    //元数据区域严格按照下面的结构
    private final long readPosition;
    private final long uploadPosition;
    private final long updateTime;

    public FileMetaInfo(long readPosition, long uploadPosition, long updateTime) {
        this.readPosition = readPosition;
        this.uploadPosition = uploadPosition;
        this.updateTime = updateTime;
    }

    public long getReadPosition() {
        return readPosition;
    }

    public long getUploadPosition() {
        return uploadPosition;
    }

    public long getUpdateTime() {
        return updateTime;
    }
}
