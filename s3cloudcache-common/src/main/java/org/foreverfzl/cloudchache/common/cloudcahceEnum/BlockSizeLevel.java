package org.foreverfzl.cloudchache.common.cloudcahceEnum;

public enum BlockSizeLevel {
    SMALL(8 * 1024 * 1024),     // 8MB
    MEDIUM(16 * 1024 * 1024),     // 16MB
    LARGE(32 * 1024 * 1024),     // 32MB
    ULTRA(64 * 1024 * 1024);        // 64MB

    private final int bytes;

    BlockSizeLevel(int bytes) {
        this.bytes = bytes;
    }

    public int getBytes() {
        return bytes;
    }
}
