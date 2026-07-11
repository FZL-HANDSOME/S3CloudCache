package org.foreverfzl.cloudcache.wal.datastruct;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public final class FileMetaInfo {

    /**
     * 当前已写入位置（已经成功写入 WAL）
     */
    private final long readPosition;

    /**
     * 已连续上传到 S3 的位置
     */
    private final long uploadPosition;

    /**
     * 文件创建时间
     */
    private final long createTime;

    /**
     * 最后更新时间
     */
    private final long lastUpdateTime;

    /**
     * Header CRC32
     */
    private int crc;

    public FileMetaInfo(long readPosition, long uploadPosition, long createTime, long lastUpdateTime) {
        this.readPosition = readPosition;
        this.uploadPosition = uploadPosition;
        this.createTime = createTime;
        this.lastUpdateTime = lastUpdateTime;
        updateCrc();
    }

    public FileMetaInfo(long readPosition, long uploadPosition, long createTime, long lastUpdateTime, int crc) {
        this.readPosition = readPosition;
        this.uploadPosition = uploadPosition;
        this.createTime = createTime;
        this.lastUpdateTime = lastUpdateTime;
        this.crc = crc;
    }

    /**
     * 更新 CRC
     */
    public void updateCrc() {
        CRC32 crc32 = new CRC32();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 4);
        buffer.putLong(readPosition);
        buffer.putLong(uploadPosition);
        buffer.putLong(createTime);
        buffer.putLong(lastUpdateTime);
        crc32.update(buffer.array());
        this.crc = (int) crc32.getValue();
    }

    /**
     * 校验 CRC
     */
    public boolean verifyCrc() {
        CRC32 crc32 = new CRC32();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 4);
        buffer.putLong(readPosition);
        buffer.putLong(uploadPosition);
        buffer.putLong(createTime);
        buffer.putLong(lastUpdateTime);
        crc32.update(buffer.array());
        return this.crc == (int) crc32.getValue();
    }


}
