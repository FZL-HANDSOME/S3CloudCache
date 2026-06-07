package org.foreverfzl.cloudchache.common.config;

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
        public static WalFileSize walFileSize = WalFileSize.SIZE_1G;

    }


    /**
     * 缓冲区配置
     */
    private static class CacheConfig {

    }

    public static void setIsLockMappedFilePageCache(boolean isLockMappedFilePageCache) {
        WalConfig.isLockMappedFilePageCache = isLockMappedFilePageCache;
    }

    public static boolean getIsLockMappedFilePageCache() {
        return WalConfig.isLockMappedFilePageCache ;
    }

    public static void setWalFileSize(WalFileSize walFileSize) {
        WalConfig.walFileSize = walFileSize;
    }

    public static WalFileSize getWalFileSize() {
        return WalConfig.walFileSize;
    }

}

