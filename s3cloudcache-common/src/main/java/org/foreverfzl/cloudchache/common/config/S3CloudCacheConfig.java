package org.foreverfzl.cloudchache.common.config;

import org.foreverfzl.cloudchache.common.cloudcahceEnum.*;

/**
 * 该类存储真个项目的配置·
 */
public class S3CloudCacheConfig {

    private final WalConfig walConfig;
    private final CacheConfig cacheConfig;

    public S3CloudCacheConfig() {
        walConfig = new WalConfig();
        cacheConfig = new CacheConfig();
    }

    /* 持久化配置 ——————————————————————————————————————————————————————————*/

    public class WalConfig {
        /**
         * 用户指定的持久化目录
         */
        public String walPath = null;

        /**
         * 是否锁定持久化文件对应的操作系统PageCache缓冲区
         */
        public boolean isLockMappedFilePageCache = true;

        /**
         * WAL 文件大小，默认 1G
         */
        public long walFileSize = WalFileSize.SIZE_1G.getBytes();

        /**
         * ACK确认机制，默认写入到操作系统Page中就返回成功
         */
        public AckLevel ackLevel = AckLevel.WRITE_PAGE;

        /**
         * Page刷盘级别
         */
        public long pageFlushLevel = PageFlushLevel.FAST_10_MS.getFlushIntervalMs();

        public WalConfig setWalPath(String walPath) {
            this.walPath = walPath;
            return this;
        }

        public WalConfig setLockMappedFilePageCache(boolean lockMappedFilePageCache) {
            isLockMappedFilePageCache = lockMappedFilePageCache;
            return this;
        }

        public WalConfig setWalFileSize(long walFileSize) {
            this.walFileSize = walFileSize;
            return this;
        }

        public WalConfig setAckLevel(AckLevel ackLevel) {
            this.ackLevel = ackLevel;
            return this;
        }

        public WalConfig setPageFlushLevel(long pageFlushLevel) {
            this.pageFlushLevel = pageFlushLevel;
            return this;
        }


    }


    /* 缓冲区配置 ——————————————————————————————————————————————————————————*/

    public class CacheConfig {
        /**
         * 缓冲区大小
         */
        public long cacheSize = CacheSizeLevel.TINE.getBytes();

        /**
         * Block的大小，默认为8MB
         */
        public int blockSize = BlockSizeLevel.MEDIUM.getBytes();

        /**
         * Block并发上传大小，默认为8个
         */
        public int blockUpLoadCount = BlockUploadConcurrencyLevel.NORMAL.getConcurrency();

        public CacheConfig setCacheSize(long cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public CacheConfig setBlockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public CacheConfig setBlockUpLoadCount(int blockUpLoadCount) {
            this.blockUpLoadCount = blockUpLoadCount;
            return this;
        }

        public long getCacheSize() {
            return cacheSize;
        }

        public int getBlockSize() {
            return blockSize;
        }

        public int getBlockUpLoadCount() {
            return blockUpLoadCount;
        }
    }

    public WalConfig getWalConfig() {
        return walConfig;
    }

    public CacheConfig getCacheConfig() {
        return cacheConfig;
    }
}

