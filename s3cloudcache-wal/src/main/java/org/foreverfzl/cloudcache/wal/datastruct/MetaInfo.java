package org.foreverfzl.cloudcache.wal.datastruct;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * 一个Bucket对应的元数据 文件内容格式
 */
public class MetaInfo {

    private long fileSize;

    private int blockSize;

    private int crc;

    private final int dataLen;

    /**
     * 用户自定义 S3 Key 前缀
     */
    private final byte[] data;


    public MetaInfo(long fileSize,int blockSize,String s3KeyPrefix) {
        this.fileSize=fileSize;
        this.blockSize=blockSize;
        this.data = s3KeyPrefix.getBytes(StandardCharsets.UTF_8);
        this.dataLen = data.length;
        updateCrc();
    }

    public MetaInfo(int crc, byte[] data) {
        this.crc = crc;
        this.data = data;
        this.dataLen = data.length;
    }


    /**
     * 更新 CRC
     */
    public void updateCrc() {
        CRC32 crc32 = new CRC32();
        crc32.update(Math.toIntExact(this.fileSize));
        crc32.update(this.blockSize);
        crc32.update(dataLen);
        crc32.update(data);
        this.crc = (int) crc32.getValue();
    }

    public int getCrc() {
        return crc;
    }

    public int getDataLen() {
        return dataLen;
    }

    public byte[] getData() {
        return data;
    }

    public long getFileSize() {
        return fileSize;
    }

    public int getBlockSize() {
        return blockSize;
    }
}
