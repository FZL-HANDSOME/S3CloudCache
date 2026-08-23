package org.foreverfzl.cloudcache.metadata.entity;

public class RecoverTask {

    private final long fileFromOffset;
    private final int blockIndex;
    private int times; //被取出放回的次数，如果超过3次该任务就会从队列中删除

    public RecoverTask(long fileFromOffset, int blockIndex) {
        this.fileFromOffset = fileFromOffset;
        this.blockIndex = blockIndex;
        times = 0;
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public int getBlockIndex() {
        return blockIndex;
    }

    public void incrementTimes() {
        times++;
    }

    public int getTimes() {
        return times;
    }
}
