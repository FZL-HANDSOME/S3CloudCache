package org.foreverfzl.cloudcache.metadata.manager;

import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.metadata.entity.RecoverTask;
import org.foreverfzl.cloudcache.metadata.entity.UploadTask;
import org.foreverfzl.cloudchache.common.ProjectUtil;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 该类代表一个Bucket对应的Block相关的元数据
 * 该类管理该Bucket下所有活跃的Block
 */
public class BlockMetaDataManager {

    //key为fileFromOffset+blockIndex
    private final ConcurrentHashMap<Long, BlockMetaData> metaDataMap = new ConcurrentHashMap<>();
    //该bucket对应的 N 秒检查 时间超过 M秒 的Block进行封口上传的任务管理者
    private final BlockUpLoadQueueManager blockUpLoadQueueManager = new BlockUpLoadQueueManager();

    private final BlockRecoverQueueManager blockRecoverQueueManager = new BlockRecoverQueueManager();

    public BlockMetaDataManager() {

    }

    public void chackLastActiveTime(long fileFromOffset, int blockIndex, long curTime, long maxFreeTime) {
        BlockMetaData blockMetaData = metaDataMap.get(ProjectUtil.buildBlockKey(fileFromOffset, blockIndex));
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
        blockUpLoadQueueManager.submit(new UploadTask(fileFromOffset, blockIndex));
    }

    public UploadTask getTaskFromUpLoadQueue() throws InterruptedException {
        return blockUpLoadQueueManager.take();
    }

    public RecoverTask getTaskFromRecoverQueue() throws InterruptedException {
        return blockRecoverQueueManager.take();
    }

    //将对应的元数据设置为Broke
    public void setMetaDataBroken(long fileFromOffset, int blockIndex) {
        BlockMetaData blockMetaData = metaDataMap.get(ProjectUtil.buildBlockKey(fileFromOffset, blockIndex));
        blockMetaData.setBroken();
    }

    /**
     * 获取或者创建BlockMetaData
     */
    public BlockMetaData getOrCreate(long fileFromOffset, int blockIndex) {
        return metaDataMap.computeIfAbsent(ProjectUtil.buildBlockKey(fileFromOffset, blockIndex), k -> new BlockMetaData());
    }

    /**
     * 获取BlockMetaData
     */
    public BlockMetaData get(long fileFromOffset, int blockIndex) {
        return metaDataMap.get(ProjectUtil.buildBlockKey(fileFromOffset, blockIndex));
    }

    /**
     * 删除BlockMetaData
     */
    public void remove(long fileFromOffset, int blockIndex) {
        metaDataMap.remove(ProjectUtil.buildBlockKey(fileFromOffset, blockIndex));
    }

    /**
     * 增加期待写入字节数
     */
    public void addExpectedBytes(long fileFromOffset, int blockIndex, int bytes) {
        BlockMetaData blockMetaData = getOrCreate(fileFromOffset, blockIndex);
        blockMetaData.addExpectedBytes(bytes);
        blockMetaData.updateLastTime();
    }

    /**
     * 增加已经完成写入字节数
     */
    public void addFinishedBytes(long fileFromOffset, int blockIndex, int bytes) {
        get(fileFromOffset, blockIndex).addFinishedBytes(bytes);
    }

    /**
     * CAS封口
     */
    public void trySeal(long fileFromOffset, int blockIndex) {
        BlockMetaData blockMetaData = get(fileFromOffset, blockIndex);
        boolean b = blockMetaData.trySeal();
        if (b && blockMetaData.isBroken()) {
            blockRecoverQueueManager.submit(new RecoverTask(fileFromOffset, blockIndex));
        }
    }


    /**
     * CAS改为上传中
     */
    public void tryStartUpload(long fileFromOffset, int blockIndex) {
        get(fileFromOffset, blockIndex).tryStartUpload();
    }


    /**
     * CAS改为上传完成，也就是直接删除对应的元数据
     */
    public void markUploadSuccess(long fileFromOffset, int blockIndex) {
        this.remove(fileFromOffset, blockIndex);
    }

    /**
     * CAS改为上传失败
     */
    public void markUploadFailed(long fileFromOffset, int blockIndex) {
        get(fileFromOffset, blockIndex).markUploadFailed();
    }

    /**
     * CAS改将上传失败改为上传中
     */
    public void retryUpload(long fileFromOffset, int blockIndex) {
        get(fileFromOffset, blockIndex).retryUpload();
    }

    /**
     * 是否已经封口
     */
    public boolean isSealed(long fileFromOffset, int blockIndex) {
        BlockMetaData metaData = get(fileFromOffset, blockIndex);
        return metaData != null && metaData.getState() == 1;
    }

    /**
     * 是否已经全部写入完成，可以上传
     */
    public boolean canUpload(long fileFromOffset, int blockIndex) {
        BlockMetaData metaData = get(fileFromOffset, blockIndex);
        if (metaData == null) {
            return false;
        }
        return metaData.getState() == 1
                && metaData.getExpectedBytes() == metaData.getFinishedBytes();
    }

}
