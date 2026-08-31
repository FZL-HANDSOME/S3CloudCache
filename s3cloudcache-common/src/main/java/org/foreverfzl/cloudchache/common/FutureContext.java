package org.foreverfzl.cloudchache.common;

import java.util.concurrent.CompletableFuture;

/**
 * 调用Write方法的上下文对象
 */
public final class FutureContext {
    private long walRecordId;
    private String s3Key;
    private long physicalOffset = -1;
    private int size;
    private CompletableFuture<WriteResult> future;


    public FutureContext(CompletableFuture<WriteResult> future) {
        this.future = future;
    }

    public void setWalRecordId(long walRecordId) {
        this.walRecordId = walRecordId;
    }

    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }

    public void setPhysicalOffset(long physicalOffset) {
        this.physicalOffset = physicalOffset;
    }

    public void setSize(int size) {
        this.size = size;
    }


    public long getWalRecordId() {
        return walRecordId;
    }

    public String getS3Key() {
        return s3Key;
    }

    public long getPhysicalOffset() {
        return physicalOffset;
    }

    public int getSize() {
        return size;
    }

    public CompletableFuture<WriteResult> getFuture() {
        return future;
    }
}
