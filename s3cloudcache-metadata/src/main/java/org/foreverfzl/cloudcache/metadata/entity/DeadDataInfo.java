package org.foreverfzl.cloudcache.metadata.entity;

/**
 * 如果一个物理block上传三次都上传失败，则会包装成这个对象放入到死信队列中
 */
public class DeadDataInfo {

    private final String instanceName;
    private final String bucketName;
    private final long fileFromOffset;
    private final int logicalIndex;
    private final String s3Key;

    public DeadDataInfo(String instanceName, String bucketName, long fileFromOffset, int logicalIndex, String s3Key) {
        this.instanceName = instanceName;
        this.bucketName = bucketName;
        this.fileFromOffset = fileFromOffset;
        this.logicalIndex = logicalIndex;
        this.s3Key = s3Key;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getBucketName() {
        return bucketName;
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public int getLogicalIndex() {
        return logicalIndex;
    }

    public String getS3Key() {
        return s3Key;
    }
}
