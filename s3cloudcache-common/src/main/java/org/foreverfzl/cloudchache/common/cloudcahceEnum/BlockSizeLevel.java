package org.foreverfzl.cloudchache.common.cloudcahceEnum;

public enum BlockSizeLevel {
    MEDIUM(8 * 1024 * 1024),     // 8MB
    LARGE(16 * 1024 * 1024),     // 16MB
    ULTRA(32 * 1024 * 1024),     // 32MB
    MAX(64 * 1024 * 1024);        // 64MB

    private final int bytes;

    BlockSizeLevel(int bytes) {
        this.bytes = bytes;
    }

    public int getBytes() {
        return bytes;
    }
}
