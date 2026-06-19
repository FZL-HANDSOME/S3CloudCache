package org.foreverfzl.cloudchache.common.cloudcahceEnum;

/**
 * WAL持久化文件大小枚举类
 */
public enum WalFileSize {
    SIZE_256MB(256L * 1024 * 1024),      // 256MB
    SIZE_512MB(512L * 1024 * 1024),      // 512MB
    SIZE_1G(1024L * 1024 * 1024),       // 1GB
    SIZE_2G(2L * 1024 * 1024 * 1024);   // 2GB

    private final long bytes;

    WalFileSize(long bytes) {
        this.bytes = bytes;
    }

    public long getBytes() {
        return bytes;
    }
}
