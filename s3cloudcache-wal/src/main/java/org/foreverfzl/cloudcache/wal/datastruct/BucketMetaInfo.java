package org.foreverfzl.cloudcache.wal.datastruct;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * 一个Bucket对应的元数据 文件内容格式
 */
public class BucketMetaInfo {

    //是否需要数据恢复，1代表需要，0代表不需要
    private int isDirty;

    private int blockSize;

    private long fileSize;

    private int crc;

    private final int dataLen;

    /**
     * 用户自定义 S3 Key 前缀
     */
    private final byte[] data;


    public BucketMetaInfo(int blockSize,long fileSize,  String s3KeyPrefix) {
        this.isDirty = 1;
        this.blockSize = blockSize;
        this.fileSize = fileSize;
        this.data = s3KeyPrefix.getBytes(StandardCharsets.UTF_8);
        this.dataLen = data.length;
        updateCrc();
    }

    public BucketMetaInfo(int isDirty, int blockSize, long fileSize, byte[] data) {
        this.isDirty = isDirty;
        this.fileSize = fileSize;
        this.blockSize = blockSize;
        this.data = data;
        this.dataLen = data.length;
    }


    /**
     * 更新 CRC
     */
    public void updateCrc() {
        CRC32 crc32 = new CRC32();
        crc32.update(this.blockSize);
        crc32.update(Math.toIntExact(this.fileSize));
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

    public int getIsDirty() {
        return isDirty;
    }

    public long getFileSize() {
        return fileSize;
    }

    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public String toString() {
        return "BucketMetaInfo{" +
                "isDirty=" + isDirty +
                ", blockSize=" + blockSize +
                ", fileSize=" + fileSize +
                ", crc=" + crc +
                ", dataLen=" + dataLen +
                ", data=" + Arrays.toString(data) +
                '}';
    }
}
