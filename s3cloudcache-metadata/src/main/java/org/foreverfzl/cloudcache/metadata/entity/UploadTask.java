package org.foreverfzl.cloudcache.metadata.entity;

/**
 * 上传任务实体
 */
public class UploadTask {
    //文件名字
    private final String fileName;
    //block逻辑索引
    private final int logicalIndex;

    public UploadTask(String fileName, int logicalIndex) {

        this.fileName = fileName;
        this.logicalIndex = logicalIndex;
    }



    public String getFileName() {
        return fileName;
    }

    public int getLogicalIndex() {
        return logicalIndex;
    }
}
