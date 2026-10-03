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
/**
 * 中文：按 bucket 管理逻辑块状态及上传、恢复、死信队列；队列项只提供逻辑定位，不持有 WAL 或物理块引用，消费方必须重新校验资格。
 * English: Manages logical-block state and upload, recovery, and dead-letter queues per bucket; queue entries carry logical coordinates rather than WAL or physical-block references, so consumers must revalidate eligibility.
 */
public class BlockMetaDataManager {

    /**
     * 中文：恢复任务消费等元数据调度日志。
     * English: Logs metadata scheduling events such as recovery-task consumption.
     */
    private static final Logger log = LoggerFactory.getLogger(BlockMetaDataManager.class);
    //key为fileFromOffset+blockIndex
    /**
     * 中文：按编码的文件逻辑偏移与块序号索引共享 metadata；同一条目对象还承担跨层监视锁角色。
     * English: Shared metadata indexed by encoded logical file offset and block index; each entry also serves as a cross-layer monitor.
     */
    private final ConcurrentHashMap<Long, BlockMetaData> metaDataMap = new ConcurrentHashMap<>();
    //    //该bucket对应的 N 秒检查 时间超过 M秒 的Block进行封口上传的任务管理者
    /**
     * 中文：上传候选队列，允许重复项，不代表候选已取得上传资格。
     * English: Upload-candidate queue allowing duplicates; enqueueing does not grant upload admission.
     */
    private final BlockUpLoadQueue blockUpLoadQueue = new BlockUpLoadQueue();
    //存放物理block写入失败，读取wal文件重新恢复的信息
    /**
     * 中文：损坏物理块的 WAL 重放候选队列；初次提交去重由 metadata 位控制。
     * English: Queue of WAL-replay candidates for broken physical blocks; a metadata bit deduplicates initial submission.
     */
    private final BlockRecoverQueue blockRecoverQueue = new BlockRecoverQueue();
    //存放物理block上传不上去的数据
    /**
     * 中文：失败逻辑块的诊断定位队列，无 payload，也不负责自动重试或删除 WAL。
     * English: Diagnostic locators for failed logical blocks, containing no payload and performing no automatic retry or WAL deletion.
     */
    private final DeadDataQueue deadDataQueue = new DeadDataQueue();

    /**
     * 中文：创建空的 bucket 元数据注册表与队列，不启动后台线程。
     * English: Creates an empty bucket metadata registry and queues without starting background threads.
     */
    public BlockMetaDataManager() {

    }

    //该方法返回true则代表成功的将最后一个长时间不写block封口
    /**
     * 中文：检查仍开放的块是否超过空闲阈值并尝试封口；补充：返回 trySeal 的位掩码，不是布尔值，也不自行入队或刷盘。
     * English: Checks whether an open block exceeds the idle threshold and attempts sealing; returns the trySeal bit mask, not a boolean, and does not itself enqueue or force storage.
     * @param blockMetaData 待检查 metadata，可为 null；English: metadata to inspect, nullable
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     * @param curTime 当前 epoch 毫秒时间；English: current epoch milliseconds
     * @param maxFreeTime 空闲阈值，单位毫秒；English: idle threshold in milliseconds
     * @return 未触发时为 0，否则为封口检查位掩码；English: zero when not triggered, otherwise the sealing-check bit mask
     */
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

    /**
     * 中文：阻塞取出一个失败定位项，仅消费队列，不读取或删除失败数据。
     * English: Blocks for one failure locator, consuming only the queue entry without reading or deleting failed data.
     * @return 下一项失败定位；English: next failure locator
     * @throws InterruptedException 等待过程中被中断；English: interrupted while waiting
     */
    public DeadDataInfo getDeadDataInfo() throws InterruptedException {
        return deadDataQueue.take();
    }

    /**
     * 中文：阻塞获取上传候选，调用方还需验证绑定、字节数和状态。
     * English: Blocks for an upload candidate; the caller must still verify binding, byte totals, and state.
     * @return 下一项上传候选；English: next upload candidate
     * @throws InterruptedException 等待过程中被中断；English: interrupted while waiting
     */
    public UploadTask getTaskFromUpLoadQueue() throws InterruptedException {
        return blockUpLoadQueue.take();
    }

    /**
     * 中文：阻塞获取恢复候选并记录定位信息；出队不代表已取得独占恢复租约。
     * English: Blocks for a recovery candidate and logs its coordinates; dequeueing does not acquire an exclusive recovery lease.
     * @return 下一项恢复候选；English: next recovery candidate
     * @throws InterruptedException 等待过程中被中断；English: interrupted while waiting
     */
    public RecoverTask getTaskFromRecoverQueue() throws InterruptedException {
        RecoverTask take = blockRecoverQueue.take();
        log.info("fileFromOffset=>{}, blockIndex=>{} is Consumed from recoverQueue", take.getFileFromOffset(), take.getBlockIndex());
        return take;
    }


    /**
     * 中文：将同一恢复候选重新排队等待条件齐全；不递增 times、不去重，也不据重排次数丢弃 WAL。
     * English: Requeues the same recovery candidate while awaiting readiness; does not increment times, deduplicate, or discard WAL based on requeue count.
     * @param recoverTask 要重新排队的候选；English: candidate to requeue
     */
    public void reSetTaskToRecoverQueue(RecoverTask recoverTask) {
        blockRecoverQueue.submit(recoverTask);
    }

    /**
     * 中文：仅提交指定逻辑块的上传候选，不检查状态，也不防止重复提交。
     * English: Submits an upload candidate for the specified logical block without checking state or preventing duplicates.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     */
    public void setTaskToUpdateQueue(long fileFromOffset, int blockIndex) {
        blockUpLoadQueue.submit(new UploadTask(fileFromOffset, blockIndex));
    }

    /**
     * 中文：仅移除一个状态条目，不完成 Future、取消任务或释放资源；调用方必须确保不会破坏仍在使用的共享身份。
     * English: Removes only one state entry without completing Futures, canceling tasks, or releasing resources; callers must preserve identities still in use.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     */
    public void deleteBlockMetaData(long fileFromOffset, int blockIndex) {
        long key = ProjectUtil.buildBlockKey(fileFromOffset, blockIndex);
        metaDataMap.remove(key);
    }

    /**
     * 中文：扫描 BlockKey 低十位所能表示的全部 1024 个索引，删除指定文件的稀疏元数据；不因索引空洞提前退出，也不取消队列任务。
     * English: Scans all 1024 indices representable by the low ten BlockKey bits to remove sparse metadata for a file; does not stop at gaps or cancel queued tasks.
     * @param fileFromOffset 已具备清理条件的文件逻辑起始字节偏移；English: logical start in bytes of a file eligible for cleanup
     */
    public void deleteFileAllBlockMetaData(long fileFromOffset) {
        // 重启恢复会跳过已经上传的块，元数据索引可能是 1、3、7，而不是从 0 连续存在。
        // BlockKey 的低 10 位最多表示 1024 个索引；遍历这个固定小范围，不能遇空洞就停止。
        // 中文：重启跳过已确认块会留下稀疏索引，必须遍历完整编码范围而非遇到空洞即停止。
        // English: Skipping acknowledged blocks during restart leaves sparse indices, requiring the full encoded range rather than stopping at gaps.
        for (int blockIndex = 0; blockIndex < 1024; blockIndex++) {
            metaDataMap.remove(ProjectUtil.buildBlockKey(fileFromOffset, blockIndex));
        }
    }


    /**
     * 获取或者创建BlockMetaData
     */
    /**
     * 中文：原子获取或创建开放状态的逻辑块 metadata，返回对象也作为跨层同步身份。
     * English: Atomically obtains or creates open logical-block metadata, also used as the cross-layer synchronization identity.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     * @return 共享 metadata 对象；English: shared metadata object
     */
    public BlockMetaData getOrCreate(long fileFromOffset, int blockIndex) {
        return metaDataMap.computeIfAbsent(ProjectUtil.buildBlockKey(fileFromOffset, blockIndex), k -> new BlockMetaData());
    }

    /**
     * 获取BlockMetaData
     */
    /**
     * 中文：只查找已有 metadata，不创建、不固定其关联资源生命周期。
     * English: Looks up existing metadata without creating it or pinning associated resource lifetimes.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     * @return 已有条目或 null；English: existing entry or null
     */
    public BlockMetaData getBlockMetaData(long fileFromOffset, int blockIndex) {
        return metaDataMap.get(ProjectUtil.buildBlockKey(fileFromOffset, blockIndex));
    }

    /**
     * 增加期待写入字节数
     */
    /**
     * 中文：创建或获取 metadata 后累计 WAL 预留 value 字节；调用者需另行保证预留与封口的同步协议。
     * English: Gets or creates metadata and adds reserved WAL value bytes; callers separately enforce reservation/sealing synchronization.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     * @param bytes 预留的 value 字节数，不含 WAL 头；English: reserved value bytes excluding WAL headers
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
    /**
     * 中文：累计 WAL 映射复制成功的 value 字节并更新时间；补充：实际返回 metadata 而非布尔值，此方法不封口、刷盘或更新文件读指针。
     * English: Adds value bytes copied successfully to the WAL mapping and updates activity time; returns metadata rather than a boolean and does not seal, force storage, or advance file read pointers.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     * @param bytes 已复制的 value 字节数；English: copied value bytes
     * @return 已更新的 metadata，缺失时为 null；English: updated metadata, or null when absent
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
    /**
     * 中文：仅检查非 OPEN 状态且预留量等于 WAL 复制量；FAILED 等状态也可能满足，不代表磁盘持久化或上传资格。
     * English: Checks only non-OPEN state and equality of reserved/WAL-copied bytes; FAILED and other states may match, so this does not establish persistence or upload eligibility.
     * @param blockMetaData 非 null 的状态对象；English: non-null metadata object
     * @return 当前检查条件是否成立；English: whether the current condition holds
     */
    public boolean isAllDataWriteInPageCache(BlockMetaData blockMetaData) {
        if (blockMetaData.getState() != BlockMetaData.OPEN
                && blockMetaData.getExpectedBytes() == blockMetaData.getPageCacheBytes()) {
            return true;
        }
        return false;
    }

    /**
     * 增加已经完成写入字节数
     */
    /**
     * 中文：为已有 metadata 累计物理块成功复制字节，条目缺失时忽略，不自动创建。
     * English: Adds successful physical-copy bytes to existing metadata, ignoring missing entries rather than creating them.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     * @param bytes 物理块已复制的 value 字节数；English: value bytes copied to the physical block
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
    /**
     * 中文：查找 metadata 后尝试封口并返回后续动作候选位掩码，不直接执行候选动作。
     * English: Looks up metadata, attempts sealing, and returns candidate-action bits without executing those actions.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     * @return 0 或以下位的组合：1 为 WAL 复制量齐，2 为可上传，4 为待恢复；English: zero or a combination of 1 for complete WAL copy accounting, 2 for upload eligibility, and 4 for recovery candidacy
     */
    public int trySeal(long fileFromOffset, int blockIndex) {
        return trySeal(fileFromOffset, blockIndex, null);
    }


    /**
     * 中文：尝试 OPEN 到 SEALED，并返回可组合的检查位：位 0 仅表示 expected=pageCache，位 1 表示上传条件齐全，位 2 表示封口损坏且尚未提交恢复；返回是候选快照，消费者必须复核。
     * English: Attempts OPEN-to-SEALED and returns combinable checks: bit 0 only means expected=pageCache, bit 1 means upload eligibility, and bit 2 means sealed/broken without recovery submission. These are candidate snapshots requiring consumer revalidation.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     * @param blockMetaData 可复用的 metadata，null 时按坐标查询；English: supplied metadata, looked up by coordinates when null
     * @return 检查位掩码，缺失条目为 0；English: check bit mask, or zero for a missing entry
     */
    public int trySeal(long fileFromOffset, int blockIndex,BlockMetaData blockMetaData) {
        if (blockMetaData == null) {
            blockMetaData = getBlockMetaData(fileFromOffset, blockIndex);
        }
        if (blockMetaData == null) return 0;
        int ans = 0;
        synchronized (blockMetaData) {
            //seal和broken状态存在竞争关系
            // 中文：封口与 broken 标记共用该 metadata 锁；封口并不意味着在途复制已经结束。
            // English: Sealing and broken marking share this metadata monitor; sealing does not mean in-flight copies have finished.
            blockMetaData.trySeal();
            //wal文件写完了，检测一下物理block是否破损，如果破损则可以开始恢复数据
            if (blockMetaData.getState() == BlockMetaData.SEALED
                    && blockMetaData.isBroken() && !blockMetaData.isBrokenSubmit()) {
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
    /**
     * 中文：对已封口、损坏且未在恢复的块首次提交恢复候选，并设置已提交位；补充：此方法不把健康块标为 broken，也不取得物理恢复租约。
     * English: Initially submits recovery for a sealed, broken, nonrecovering block and sets its submitted bit; does not mark healthy blocks broken or acquire physical recovery leases.
     * @param blockMetaData 对应逻辑块的共享 metadata；English: shared metadata for the logical block
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     */
    public void setTaskToRecoverQueue(final BlockMetaData blockMetaData, long fileFromOffset, int blockIndex) {
        synchronized (blockMetaData) {
            if (blockMetaData.getState() != BlockMetaData.SEALED || !blockMetaData.isBroken()
                    || blockMetaData.isRecovering()) {
                return;
            }
            int isBroken = blockMetaData.getIsBroken();
            if ((isBroken & (1 << 1)) == (1 << 1)) {
                return;
            }
            //将恢复任务放入到队列中，并将对应位置标记为已经提交，幂等性控制
            // 中文：先放入候选再置位，初次提交在同一 metadata 锁内去重；重新排队由另一个入口负责。
            // English: Enqueue the candidate before setting its bit; initial submissions are deduplicated under this monitor, while requeueing has a separate entry.
            blockRecoverQueue.submit(new RecoverTask(fileFromOffset, blockIndex));
            blockMetaData.setBrokenSubmit();
        }
    }


    /**
     * CAS改为上传中
     */
    /**
     * 中文：委托 metadata 检查逻辑条件并切换上传状态，不负责物理引用或执行任务。
     * English: Delegates logical eligibility checking and upload-state transition without managing physical references or executing a task.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     * @return 是否领取到上传状态，缺失条目为 false；English: whether upload state was claimed, false when absent
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
    /**
     * 中文：将已有条目从 UPLOADING 尝试改为 SUCCESS；更正旧说明：不删除 metadata，不写 WAL 确认，也不通知 Future。
     * English: Attempts UPLOADING-to-SUCCESS on an existing entry; contrary to the older description, this does not remove metadata, persist WAL acknowledgement, or notify Futures.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
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
    /**
     * 中文：仅在首次转换为 FAILED 时登记一个死信定位；条目缺失、已成功或已失败则不重复登记，Future 通知由任务拥有者另行负责。
     * English: Registers one dead-letter locator only when transitioning newly to FAILED; missing, successful, or already-failed entries produce no duplicate, and the task owner handles Future notification separately.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     * @param deadDataInfo 失败逻辑块的诊断定位；English: diagnostic locator for the failed logical block
     */
    public void markUploadFailed(long fileFromOffset, int blockIndex, DeadDataInfo deadDataInfo) {
        BlockMetaData blockMetaData = getBlockMetaData(fileFromOffset, blockIndex);
        if (blockMetaData == null) {
            return;
        }
        if (blockMetaData.markUploadFailed()) {
            deadDataQueue.submit(deadDataInfo);
        }
    }

    /**
     * CAS改将上传失败改为上传中
     */
    /**
     * 中文：仅尝试 FAILED 到 UPLOADING 状态转换，不检查资源齐备、不重新提交队列或恢复已通知的 Future。
     * English: Only attempts FAILED-to-UPLOADING without checking resource readiness, resubmitting work, or restoring already-notified Futures.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
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
    /**
     * 中文：实际判断主状态是否非 OPEN，包含 UPLOADING/SUCCESS/FAILED；补充：不直接检查 broken 位，OPEN 且 broken 时仍返回 false。
     * English: Actually checks whether primary state is non-OPEN, including UPLOADING/SUCCESS/FAILED; does not inspect broken directly, so OPEN plus broken still returns false.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     * @return 条目存在且主状态非 OPEN；English: entry exists and primary state is non-OPEN
     */
    public boolean isSealed(long fileFromOffset, int blockIndex) {
        BlockMetaData metaData = getBlockMetaData(fileFromOffset, blockIndex);
        if (metaData == null) return false;
        return metaData.getState() != 0;
    }


    /**
     * 是否已经全部写入完成，可以上传
     */
    /**
     * 中文：查询已有 metadata 的逻辑上传条件；不增加物理引用，调用方还需防止绑定复用与状态变化。
     * English: Queries logical upload eligibility of existing metadata without adding a physical reference; callers must still guard against binding reuse and state changes.
     * @param fileFromOffset 文件逻辑起始字节偏移；English: logical file start in bytes
     * @param blockIndex 文件内逻辑块序号；English: logical block index
     * @return 当前逻辑条件是否满足，缺失条目为 false；English: whether current logical conditions hold, false when absent
     */
    public boolean canUpload(long fileFromOffset, int blockIndex) {
        BlockMetaData metaData = getBlockMetaData(fileFromOffset, blockIndex);
        if (metaData == null) {
            return false;
        }
        return metaData.canUpload();
    }

    //将所有open的block封口
    /**
     * 中文：尝试将当前遍历到的 OPEN metadata 封口；不写 WAL 结束标记、不更新文件就绪标志，也不提交队列任务，不能单独替代 WAL 的封口流程。
     * English: Attempts to seal OPEN entries seen during traversal; does not write WAL end markers, update file readiness, or enqueue tasks, so it cannot replace the WAL sealing workflow on its own.
     */
    public void trySealAllBlock() {
        for (BlockMetaData metaData : metaDataMap.values()) {
            if (metaData.getState() == BlockMetaData.OPEN) {
                metaData.trySeal();
            }
        }
    }

    /**
     * 中文：检查是否有正在恢复或已封口损坏的状态快照；不等待、不提交任务，停机方需先控制新工作准入。
     * English: Checks a snapshot for active recovery or sealed/broken entries without waiting or submitting work; shutdown callers must first control new admission.
     * @return 是否观察到待处理恢复状态；English: whether pending recovery state was observed
     */
    public boolean hasPendingRecovery() {
        return metaDataMap.values().stream().anyMatch(meta -> meta.isRecovering()
                || (meta.getState() == BlockMetaData.SEALED && meta.isBroken()));
    }

    /**
     * 中文：将当前非 SUCCESS 条目标为失败并异步派发 false 结果；调用方应先停稳生产者与相关任务，避免与提交并发竞争，不在此释放资源或删除 WAL。
     * English: Marks currently non-SUCCESS entries failed and asynchronously dispatches false results; callers should first quiesce producers and relevant tasks to avoid racing commit. This method neither releases resources nor deletes WAL.
     */
    public void failUncommittedFutures() {
        for (BlockMetaData meta : metaDataMap.values()) {
            if (meta.getState() != BlockMetaData.SUCCESS) {
                meta.markUploadFailed();
                meta.failAllFuture();
            }
        }
    }

}
