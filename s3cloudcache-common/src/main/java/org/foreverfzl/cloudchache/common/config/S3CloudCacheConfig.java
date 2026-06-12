package org.foreverfzl.cloudchache.common.config;

import org.foreverfzl.cloudchache.common.cloudcahceEnum.AckLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.BlockSizeLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.PageFlushLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.WalFileSize;

/**
 * 该类存储真个项目的配置·
 */
public class S3CloudCacheConfig {


    /* 持久化配置 ——————————————————————————————————————————————————————————*/

    /**
     * 是否锁定持久化文件对应的操作系统PageCache缓冲区
     */
    private boolean isLockMappedFilePageCache = true;

    /**
     * WAL 文件大小，默认 1G
     */
    private long walFileSize = WalFileSize.SIZE_1G.getBytes();

    /**
     * ACK确认机制，默认写入到操作系统Page中就返回成功
     */
    private AckLevel ackLevel = AckLevel.WRITE_PAGE;

    /**
     * Page刷盘级别
     */
    private long pageFlushLevel = PageFlushLevel.FAST_10_MS.getFlushIntervalMs();



    /* 缓冲区配置 ——————————————————————————————————————————————————————————*/

    private int blockSize = BlockSizeLevel.SMALL.getBytes();


    //-----------------------------------------------------------------------------------------

    public boolean isLockMappedFilePageCache() {
        return isLockMappedFilePageCache;
    }

    public void setLockMappedFilePageCache(boolean lockMappedFilePageCache) {
        isLockMappedFilePageCache = lockMappedFilePageCache;
    }

    public long getWalFileSize() {
        return walFileSize;
    }

    public void setWalFileSize(long walFileSize) {
        this.walFileSize = walFileSize;
    }

    public AckLevel getAckLevel() {
        return ackLevel;
    }

    public void setAckLevel(AckLevel ackLevel) {
        this.ackLevel = ackLevel;
    }

    public long getPageFlushLevel() {
        return pageFlushLevel;
    }

    public void setPageFlushLevel(long pageFlushLevel) {
        this.pageFlushLevel = pageFlushLevel;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }
}

