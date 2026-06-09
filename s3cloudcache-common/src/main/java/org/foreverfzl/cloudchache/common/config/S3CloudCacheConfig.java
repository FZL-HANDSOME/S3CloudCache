package org.foreverfzl.cloudchache.common.config;

import org.foreverfzl.cloudchache.common.cloudcahceEnum.AckLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.PageFlushLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.WalFileSize;

/**
 * 该类存储真个项目的配置·
 */
public class S3CloudCacheConfig {


    /**
     * 持久化配置
     */
    private static class WalConfig {

        /**
         * 是否锁定持久化文件对应的操作系统PageCache缓冲区
         */
        public static boolean isLockMappedFilePageCache = true;

        /**
         * WAL 文件大小，默认 1G
         */
        public static long walFileSize = WalFileSize.SIZE_1G.getBytes();

        /**
         * ACK确认机制，默认写入到操作系统Page中就返回成功
         */
        public static AckLevel ackLevel = AckLevel.WRITE_PAGE;

        /**
         * Page刷盘级别
         */
        public static long pageFlushLevel = PageFlushLevel.NORMAL_20_MS.getFlushIntervalMs();

    }


    /**
     * 缓冲区配置
     */
    private static class CacheConfig {

    }

    public static void setPageFlushLevel(PageFlushLevel pageFlushLevel) {
        WalConfig.pageFlushLevel = pageFlushLevel.getFlushIntervalMs();
    }

    public static long setPageFlushLevel() {
        return WalConfig.pageFlushLevel;
    }

    public static void setIsLockMappedFilePageCache(boolean isLockMappedFilePageCache) {
        WalConfig.isLockMappedFilePageCache = isLockMappedFilePageCache;
    }

    public static boolean getIsLockMappedFilePageCache() {
        return WalConfig.isLockMappedFilePageCache;
    }

    public static void setWalFileSize(WalFileSize walFileSize) {
        WalConfig.walFileSize = walFileSize.getBytes();
    }

    public static long getWalFileSize() {
        return WalConfig.walFileSize;
    }

    public static AckLevel getAckLevel() {
        return WalConfig.ackLevel;
    }

    public static void setAckLevel(AckLevel ackLevel) {
        WalConfig.ackLevel = ackLevel;
    }


}

