package org.foreverfzl.cloudcache.metadata.manager;

import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.metadata.entity.UploadTask;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 该类代表一个Bucket对应的Block相关的元数据
 * 该类管理该Bucket下所有活跃的Block
 */
public class BlockMetaDataManager {

    private final ConcurrentHashMap<String, BlockMetaData> metaDataMap = new ConcurrentHashMap<>();
    //该bucket对应的 N 秒检查 时间超过 M秒 的Block进行封口上传的任务管理者
    public BlockUpLoadQueueManager blockUpLoadQueueManager;

    public BlockMetaDataManager() {
        blockUpLoadQueueManager = new BlockUpLoadQueueManager();
    }

    public void chackLastActiveTime(String fileName, int blockIndex, long curTime, long maxFreeTime) {
        BlockMetaData blockMetaData = metaDataMap.get(buildKey(fileName, blockIndex));
        if (blockMetaData == null) {
            return;
        }
        if (blockMetaData.getState() != BlockMetaData.OPEN) {
            //如果不是开放状态则不检查
            return;
        }
        //计算差值
        long delta = curTime - blockMetaData.getLastActiveTime();
        if (delta <= maxFreeTime) {
            //不满足时间差
            return;
        }
        //封口然后放入到上传队列中
        blockMetaData.trySeal();
        blockUpLoadQueueManager.submit(new UploadTask( fileName, blockIndex));
    }

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
        BlockMetaData blockMetaData = getOrCreate(fileName, blockIndex);
        blockMetaData.addExpectedBytes(bytes);
        blockMetaData.updateLastTime();
    }

    /**
     * 增加已经完成写入字节数
     */
    public void addFinishedBytes(String fileName, int blockIndex, int bytes) {
        get(fileName, blockIndex).addFinishedBytes(bytes);
    }

    /**
     * CAS封口
     */
    public void trySeal(String fileName, int blockIndex) {
        get(fileName, blockIndex).trySeal();
    }


    /**
     * CAS改为上传中
     */
    public void tryStartUpload(String fileName, int blockIndex) {
        get(fileName, blockIndex).tryStartUpload();
    }


    /**
     * CAS改为上传完成
     */
    public void markUploadSuccess(String fileName, int blockIndex) {
        get(fileName, blockIndex).markUploadSuccess();
    }

    /**
     * CAS改为上传失败
     */
    public void markUploadFailed(String fileName, int blockIndex) {
        get(fileName, blockIndex).markUploadFailed();
    }

    /**
     * CAS改将上传失败改为上传中
     */
    public void retryUpload(String fileName, int blockIndex) {
        get(fileName, blockIndex).retryUpload();
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
