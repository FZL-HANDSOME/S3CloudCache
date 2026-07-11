package org.foreverfzl.cloudcache.wal.datastruct;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * 一个Bucket对应的元数据
 */

public class MetaInfo {

    public static final int MAGIC = 0x53334343; // S3CC

    /**
     * Header CRC32
     */
    private int crc;

    private final int dataLen;

    /**
     * 用户自定义 S3 Key 前缀
     */
    private final byte[] data;



    public MetaInfo(String s3KeyPrefix) {
        this.data = s3KeyPrefix.getBytes(StandardCharsets.UTF_8);
        this.dataLen=data.length;
        updateCrc();
    }

    public MetaInfo(int crc,int dataLen,byte[] data) {
        this.crc=crc;
        this.dataLen=dataLen;
        this.data=data;
    }

    /**
     * 更新 CRC
     */
    public void updateCrc() {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        this.crc = (int) crc32.getValue();
    }

    /**
     * 校验 CRC
     */
    public boolean verifyCrc() {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return this.crc == (int) crc32.getValue();
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
}
