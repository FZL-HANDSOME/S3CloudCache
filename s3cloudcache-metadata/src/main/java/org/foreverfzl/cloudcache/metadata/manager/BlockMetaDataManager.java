package org.foreverfzl.cloudcache.metadata.manager;

import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 该类代表一个Bucket对应的Block相关的元数据
 * 该类管理该Bucket下所有活跃的Block
 */
public class BlockMetaDataManager {

    private final ConcurrentHashMap<String, BlockMetaData> metaDataMap = new ConcurrentHashMap<>();

    /**
     * 获取或者创建BlockMetaData
     */
    public BlockMetaData getOrCreate(String fileName, int blockIndex) {
        return metaDataMap.computeIfAbsent(buildKey(fileName, blockIndex), k -> new BlockMetaData());
    }

    /**
     * 获取BlockMetaData
     */
    public BlockMetaData get(String fileName, int blockIndex) {
        return metaDataMap.get(buildKey(fileName, blockIndex));
    }

    /**
     * 删除BlockMetaData
     */
    public void remove(String fileName, int blockIndex) {
        metaDataMap.remove(buildKey(fileName, blockIndex));
    }

    /**
     * 增加期待写入字节数
     */
    public void addExpectedBytes(String fileName, int blockIndex, int bytes) {
        getOrCreate(fileName, blockIndex).addExpectedBytes(bytes);
    }

    /**
     * 增加已经完成写入字节数
     */
    public void addFinishedBytes(String fileName, int blockIndex, int bytes) {
        getOrCreate(fileName, blockIndex).addFinishedBytes(bytes);
    }

    /**
     * CAS封口
     * true:本线程完成封口
     * false:已经被其它线程封口
     */
    public void seal(String fileName, int blockIndex) {
        getOrCreate(fileName, blockIndex).setState(1);
    }

    /**
     * 是否已经封口
     */
    public boolean isSealed(String fileName, int blockIndex) {
        BlockMetaData metaData = get(fileName, blockIndex);
        return metaData != null && metaData.getState() == 1;
    }

    /**
     * 是否已经全部写入完成，可以上传
     */
    public boolean canUpload(String fileName, int blockIndex) {
        BlockMetaData metaData = get(fileName, blockIndex);
        if (metaData == null) {
            return false;
        }
        return metaData.getState() == 1
                && metaData.getExpectedBytes() == metaData.getFinishedBytes();
    }

    /**
     * 构建唯一Key
     */
    private String buildKey(String fileName, int blockIndex) {
        return fileName + '_' + blockIndex;
    }

}
