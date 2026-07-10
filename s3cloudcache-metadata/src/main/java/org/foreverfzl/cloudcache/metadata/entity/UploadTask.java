package org.foreverfzl.cloudcache.metadata.entity;

/**
 * 上传任务实体
 */
public class UploadTask {
    //文件名字
    private final long fileFromOffset;
    //block逻辑索引
    private final int logicalIndex;

    public UploadTask(long fileName, int logicalIndex) {

        this.fileFromOffset = fileName;
        this.logicalIndex = logicalIndex;
    }



    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public int getLogicalIndex() {
        return logicalIndex;
    }
}
