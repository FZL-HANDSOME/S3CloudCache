package org.foreverfzl.cloudchache.common.cloudcahceEnum;

public enum BlockSizeLevel {
    SMALL(4 * 1024 * 1024),      // 4MB
    MEDIUM(8 * 1024 * 1024),     // 8MB
    LARGE(16 * 1024 * 1024),     // 16MB
    ULTRA(32 * 1024 * 1024);     // 32MB

    private final int bytes;

    BlockSizeLevel(int bytes) {
        this.bytes = bytes;
    }

    public int getBytes() {
        return bytes;
    }
}
