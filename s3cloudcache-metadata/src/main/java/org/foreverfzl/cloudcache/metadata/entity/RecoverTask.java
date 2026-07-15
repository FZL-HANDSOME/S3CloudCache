package org.foreverfzl.cloudcache.metadata.entity;

public class RecoverTask {

    private final long fileFromOffset;
    private final int blockIndex;

    public RecoverTask(long fileFromOffset, int blockIndex) {
        this.fileFromOffset = fileFromOffset;
        this.blockIndex = blockIndex;
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public int getBlockIndex() {
        return blockIndex;
    }
}
