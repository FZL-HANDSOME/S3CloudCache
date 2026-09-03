package org.foreverfzl.cloudchache.common.cloudcahceEnum;

public enum BlockSizeLevel {

    TINY(2 * 1024 * 1024),
    SMALL(4 * 1024 * 1024),
    MEDIUM(8 * 1024 * 1024),
    LARGE(16 * 1024 * 1024),
    ULTRA(32 * 1024 * 1024);

    private final int bytes;

    BlockSizeLevel(int bytes) {
        this.bytes = bytes;
    }

    public int getBytes() {
        return bytes;
    }
}
