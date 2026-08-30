package org.foreverfzl.cloudcache.metadata.manager;

import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.metadata.entity.DeadDataInfo;
import org.foreverfzl.cloudcache.metadata.entity.RecoverTask;
import org.foreverfzl.cloudcache.metadata.entity.UploadTask;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 该类代表一个Bucket对应的Block相关的元数据
 * 该类管理该Bucket下所有活跃的Block
 */
public class BlockMetaDataManager {

    private static final Logger log = LoggerFactory.getLogger(BlockMetaDataManager.class);
    //key为fileFromOffset+blockIndex
    private final ConcurrentHashMap<Long, BlockMetaData> metaDataMap = new ConcurrentHashMap<>();
    //    //该bucket对应的 N 秒检查 时间超过 M秒 的Block进行封口上传的任务管理者
    private final BlockUpLoadQueue blockUpLoadQueue = new BlockUpLoadQueue();
    //存放物理block写入失败，读取wal文件重新恢复的信息
    private final BlockRecoverQueue blockRecoverQueue = new BlockRecoverQueue();
    //存放物理block上传不上去的数据
    private final DeadDataQueue deadDataQueue = new DeadDataQueue();

    public BlockMetaDataManager() {

    }

    //该方法返回true则代表成功的将最后一个长时间不写block封口
    public int chackLastActiveTime(BlockMetaData blockMetaData, long fileFromOffset, int blockIndex, long curTime, long maxFreeTime) {
        if (blockMetaData == null) {
            return 0;
        }
        if (blockMetaData.getState() != BlockMetaData.OPEN) {
            //如果不是开放状态则不检查
            return 0;
        }
        //计算差值
        long delta = curTime - blockMetaData.getLastActiveTime();
        if (delta <= maxFreeTime) {
            //不满足时间差
            return 0;
        }
        //满足时间差封口，上传
        return trySeal(fileFromOffset, blockIndex, blockMetaData);
    }

    public DeadDataInfo getDeadDataInfo() throws InterruptedException {
        return deadDataQueue.take();
    }

    public UploadTask getTaskFromUpLoadQueue() throws InterruptedException {
        return blockUpLoadQueue.take();
    }

    public RecoverTask getTaskFromRecoverQueue() throws InterruptedException {
        RecoverTask take = blockRecoverQueue.take();
        log.info("fileFromOffset=>{}, blockIndex=>{} is Consumed from recoverQueue", take.getFileFromOffset(), take.getBlockIndex());
        return take;
    }


    public void reSetTaskToRecoverQueue(RecoverTask recoverTask) {
        blockRecoverQueue.submit(recoverTask);
    }

    public void setTaskToUpdateQueue(long fileFromOffset, int blockIndex) {
        blockUpLoadQueue.submit(new UploadTask(fileFromOffset, blockIndex));
    }

    public void deleteBlockMetaData(long fileFromOffset, int blockIndex) {
        long key = ProjectUtil.buildBlockKey(fileFromOffset, blockIndex);
        metaDataMap.remove(key);
    }

    public void deleteFileAllBlockMetaData(long fileFromOffset) {
        int blockIndex = 0;
        while (true) {
            Long key = ProjectUtil.buildBlockKey(fileFromOffset, blockIndex);
            if (!metaDataMap.containsKey(key)) {
                return;
            }
            metaDataMap.remove(key);
            blockIndex++;
        }
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
    public BlockMetaData getBlockMetaData(long fileFromOffset, int blockIndex) {
        return metaDataMap.get(ProjectUtil.buildBlockKey(fileFromOffset, blockIndex));
    }

    /**
     * 增加期待写入字节数
     */
    public void addExpectedBytes(long fileFromOffset, int blockIndex, int bytes) {
        BlockMetaData blockMetaData = getOrCreate(fileFromOffset, blockIndex);
        if (blockMetaData == null) {
            return;
        }
        blockMetaData.addExpectedBytes(bytes);
    }


    /**
     * 增加写入到PageCache的字节数，如果封口并且PageCache字节数==的期望字节数 说明该BLOCK可以被专门的先刷盘更新read指针
     * 该方法返回true则代表可以将对应文件的对应block设置为1(可以刷新状态)
     * 这里只保证了正常情况下block的检查，项目close和N秒后不写入自动封口这两种情况不在这个方法考虑范围内
     */
    public BlockMetaData addPageCacheBytes(long fileFromOffset, int blockIndex, int bytes) {
        BlockMetaData blockMetaData = getBlockMetaData(fileFromOffset, blockIndex);
        if (blockMetaData == null) {
            return null;
        }
        blockMetaData.addPageCacheBytes(bytes);
        blockMetaData.updateLastTime();
        return blockMetaData;
    }

    //检查该block的元数据，看看是否全部数据写入到操作系统的PageCache中
    public boolean isAllDataWriteInPageCache(BlockMetaData blockMetaData) {
        if (blockMetaData.getState() == BlockMetaData.SEALED
                && blockMetaData.getExpectedBytes() == blockMetaData.getPageCacheBytes()) {
            return true;
        }
        return false;
    }

    /**
     * 增加已经完成写入字节数
     */
    public void addFinishedBytes(long fileFromOffset, int blockIndex, int bytes) {
        BlockMetaData blockMetaData = getBlockMetaData(fileFromOffset, blockIndex);
        if (blockMetaData == null) {
            return;
        }
        blockMetaData.addFinishedBytes(bytes);
    }

    /**
     * CAS封口，并检查block的状态，从而进行不同的操作
     * 返回0代表 方法正常结束。
     * 返回1代表需要将文件中的数组对应位置设置为1
     */
    public int trySeal(long fileFromOffset, int blockIndex) {
        return trySeal(fileFromOffset, blockIndex, null);
    }


    public int trySeal(long fileFromOffset, int blockIndex, BlockMetaData blockMetaData) {
        if (blockMetaData == null) {
            blockMetaData = getBlockMetaData(fileFromOffset, blockIndex);
        }
        if (blockMetaData.getState() == BlockMetaData.SEALED) {
            return 0;
        }
        int ans = 0;
        synchronized (blockMetaData) {
            if (blockMetaData.getState() == BlockMetaData.SEALED) {
                return 0;
            }
            //seal和broken状态存在竞争关系
            blockMetaData.trySeal();
            //wal文件写完了，检测一下物理block是否破损，如果破损则可以开始恢复数据
            if (blockMetaData.isBroken() && !blockMetaData.isBrokenSubmit()) {
                //如果破损了则将2位置设置为1
                ans = ans | (1 << 2);
            }
        }
        if (blockMetaData.getExpectedBytes() == blockMetaData.getPageCacheBytes()) {
            //然后将文件中的数组位置改为1，代表可以更新read指针
            //如果该条件命中，则0位置设置为1
            ans = ans | 1;
        }
        //检查一下block是否可以上传
        if (canUpload(fileFromOffset, blockIndex)) {
            //如果上传了则将1位置设置为1
            ans = ans | (1 << 1);
        }
        return ans;
    }


    //将对应的元数据设置为Broke
    public void setTaskToRecoverQueue(final BlockMetaData blockMetaData, long fileFromOffset, int blockIndex) {
        synchronized (blockMetaData) {
            if (blockMetaData.getState() != BlockMetaData.SEALED) {
                return;
            }
            int isBroken = blockMetaData.getIsBroken();
            if ((isBroken & (1 << 1)) == (1 << 1)) {
                return;
            }
            //将恢复任务放入到队列中，并将对应位置标记为已经提交，幂等性控制
            blockRecoverQueue.submit(new RecoverTask(fileFromOffset, blockIndex));
            blockMetaData.setBrokenSubmit();
        }
    }


    /**
     * CAS改为上传中
     */
    public boolean tryStartUpload(long fileFromOffset, int blockIndex) {
        BlockMetaData blockMetaData = getBlockMetaData(fileFromOffset, blockIndex);
        if (blockMetaData == null) {
            return false;
        }
        return blockMetaData.tryStartUpload();
    }


    /**
     * CAS改为上传完成，也就是直接删除对应的元数据
     */
    public void markUploadSuccess(long fileFromOffset, int blockIndex) {
        BlockMetaData blockMetaData = getBlockMetaData(fileFromOffset, blockIndex);
        if (blockMetaData == null) {
            return;
        }
        blockMetaData.markUploadSuccess();
    }

    /**
     * CAS改为上传失败
     */
    public void markUploadFailed(long fileFromOffset, int blockIndex, DeadDataInfo deadDataInfo) {
        BlockMetaData blockMetaData = getBlockMetaData(fileFromOffset, blockIndex);
        if (blockMetaData == null) {
            return;
        }
        blockMetaData.markUploadFailed();
        deadDataQueue.submit(deadDataInfo);
    }

    /**
     * CAS改将上传失败改为上传中
     */
    public void retryUpload(long fileFromOffset, int blockIndex) {
        BlockMetaData blockMetaData = getBlockMetaData(fileFromOffset, blockIndex);
        if (blockMetaData == null) {
            return;
        }
        blockMetaData.retryUpload();
    }

    /**
     * 看看是否已经封口，或者对应的物理Block broken了
     */
    public boolean isSealed(long fileFromOffset, int blockIndex) {
        BlockMetaData metaData = getBlockMetaData(fileFromOffset, blockIndex);
        if (metaData == null) return false;
        return metaData.getState() != 0;
    }


    /**
     * 是否已经全部写入完成，可以上传
     */
    public boolean canUpload(long fileFromOffset, int blockIndex) {
        BlockMetaData metaData = getBlockMetaData(fileFromOffset, blockIndex);
        if (metaData == null) {
            return false;
        }
        return metaData.getState() == 1 && metaData.getExpectedBytes() == metaData.getPageCacheBytes()
                && metaData.getPageCacheBytes() == metaData.getFinishedBytes();
    }

    //将所有open的block封口
    public void trySealAllBlock() {
        for (BlockMetaData metaData : metaDataMap.values()) {
            if (metaData.getState() == BlockMetaData.OPEN) {
                metaData.trySeal();
            }
        }
    }

}
