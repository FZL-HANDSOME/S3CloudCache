package org.foreverfzl.cloudcache.metadata.entity;


import org.foreverfzl.cloudchache.common.FutureContext;
import org.foreverfzl.cloudchache.common.WriteResult;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * 中文：一个逻辑块共享的状态、value 字节计数和 Future 注册表；此对象也是 WAL 预留、core 绑定、上传准入及恢复租约共同使用的监视锁。单个原子字段操作不能替代跨字段协议。
 * English: Shared state, value-byte accounting, and Future registry for one logical block; this object is also the monitor shared by WAL reservations, core binding, upload admission, and recovery leases. Individual atomic field operations do not replace the cross-field protocol.
 */
public class BlockMetaData {

    //0代表开放，1代表封口，2代表上传中，3代表上传成功，4代表上传失败
    //           OPEN(0)
    //              │
    //              ▼
    //         SEALED(1)
    //              │
    //              ▼
    //      UPLOADING(2)
    //         │     │  ▲
    //         ▼     ▼  │
    //SUCCESS(3)   FAILED(4)
    /**
     * 中文：允许 WAL 继续预留的开放状态；尚未固定本块最终字节数。
     * English: Open state permitting further WAL reservations; the final byte total is not yet fixed.
     */
    public static final int OPEN = 0;
    /**
     * 中文：已封口但可能仍有在途复制或恢复的状态，不等于可以立即上传。
     * English: Sealed state that may still have in-flight copies or recovery; not sufficient alone for upload.
     */
    public static final int SEALED = 1;
    /**
     * 中文：上传资格已被领取的状态，含等待执行和重试阶段。
     * English: State with upload admission claimed, including execution waits and retries.
     */
    public static final int UPLOADING = 2;
    /**
     * 中文：成功终态；正常调用链在 S3 成功且 WAL 确认持久化之后才设置。
     * English: Successful terminal state; the normal call chain sets it only after S3 success and persisted WAL acknowledgement.
     */
    public static final int SUCCESS = 3;
    /**
     * 中文：当前尝试的失败终态；不仅上传，OPEN/SEALED 阶段的分配或恢复故障也可进入此状态。
     * English: Failed state for the current attempt; allocation/recovery failures can enter it from OPEN/SEALED as well as upload.
     */
    public static final int FAILED = 4;

    /**
     * 中文：主状态的原子字段更新器，由所有实例共享。
     * English: Atomic updater for primary state, shared by all instances.
     */
    private static final AtomicIntegerFieldUpdater<BlockMetaData> STATE_UPDATER;
    /**
     * 中文：取值为 OPEN 至 FAILED 的主状态，不包含独立的 broken/recovering 标志。
     * English: Primary state from OPEN through FAILED, separate from broken/recovering flags.
     */
    private volatile int state;
    //expectedBytes代表该逻辑block期望的字节数
    /**
     * 中文：WAL 预留的 value 字节总数，不含记录头、填充或文件头；预留后复制失败不会在此自动扣除。
     * English: Total value bytes reserved in WAL, excluding record headers, padding, and file metadata; copy failure does not automatically subtract a reservation.
     */
    private volatile int expectedBytes;
    /**
     * 中文：原子累计 WAL 预留 value 字节数。
     * English: Atomically accumulates reserved WAL value bytes.
     */
    private static final AtomicIntegerFieldUpdater<BlockMetaData> EXPECTED_BYTES_UPDATER;
    //pageCacheBytes代表该逻辑block写入到操作系统PageCache的字节数
    /**
     * 中文：已完成 WAL 映射复制的 value 字节数，不证明 force 或持久化已完成。
     * English: Value bytes copied into the WAL mapping; this does not establish force or persistence completion.
     */
    private volatile int pageCacheBytes;
    /**
     * 中文：原子累计成功复制到 WAL 映射的 value 字节数。
     * English: Atomically accumulates value bytes copied successfully to the WAL mapping.
     */
    private static final AtomicIntegerFieldUpdater<BlockMetaData> PAGE_CACHE_BYTES_UPDATER;
    //finishedBytes代表物理Block真正写入字节数
    /**
     * 中文：成功复制到物理块的 value 字节数，完整恢复开始时会重置。
     * English: Value bytes successfully copied to the physical block, reset when full replay starts.
     */
    private volatile int finishedBytes;
    /**
     * 中文：原子累计物理块复制完成字节数。
     * English: Atomically accumulates completed physical-block copy bytes.
     */
    private static final AtomicIntegerFieldUpdater<BlockMetaData> FINISHED_BYTES_UPDATER;
    /**
     * 中文：最近一次活跃记录的 epoch 毫秒时间；用于近似空闲判断，不要求严格单调。
     * English: Last recorded activity in epoch milliseconds; used for approximate idleness, not strict monotonic ordering.
     */
    private volatile long lastActiveTime;
    // 原请求走完 WAL -> Core 后才计入，包含物理写入失败；恢复不能越过尚未到达 Core 的请求。
    /**
     * 中文：原始 core append 已结算的 value 字节数，含失败；由本对象监视锁保护，重放不重复累计。
     * English: Settled original core-append value bytes, including failures; protected by this monitor and not counted again during replay.
     */
    private int completedAppendsBytes;
    /**
     * 中文：完整块重放是否持有恢复租约；只是状态位，设置它本身不获取物理块引用。
     * English: Whether full-block replay holds a recovery lease; setting the flag alone does not acquire a physical-block reference.
     */
    private volatile boolean recovering;

    //该isBroken指的是物理block
    /**
     * 中文：损坏及恢复已入队位的更新器；相关变更与封口共用本对象监视锁。
     * English: Updater for broken/recovery-submitted bits; mutations share this monitor with sealing.
     */
    protected static final AtomicIntegerFieldUpdater<BlockMetaData> IS_BROKEN_UPDATER;
    //二进制：第0位为 1则代表broken，第1位为1则代表已经将任务上传到队列中了
    /**
     * 中文：位 0 表示物理数据损坏，位 1 表示恢复任务已提交；补充：此处不是上传队列标志。
     * English: Bit 0 marks broken physical data and bit 1 marks recovery submission; the latter is not an upload-queue flag.
     */
    private volatile int isBroken = 0;

    //专门存放该block对应的future
    /**
     * 中文：按块内 WAL 记录字节偏移关联的原始 Future 上下文；同一记录重放更新已有上下文，不应重复登记。
     * English: Original Future contexts keyed by WAL record byte offset within the block; replay updates an existing context rather than registering it again.
     */
    ConcurrentHashMap<Long, FutureContext> futureMap = new ConcurrentHashMap<>();

    static {
        STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "state");
        EXPECTED_BYTES_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "expectedBytes");
        PAGE_CACHE_BYTES_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "pageCacheBytes");
        FINISHED_BYTES_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "finishedBytes");
        IS_BROKEN_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "isBroken");
    }

    /**
     * 中文：创建开放、零计数的逻辑块状态，初始活跃时间取当前毫秒时钟。
     * English: Creates open logical-block state with zero counters and the current millisecond clock as initial activity time.
     */
    public BlockMetaData() {
        this.state = 0;
        this.expectedBytes = 0;
        this.finishedBytes = 0;
        this.lastActiveTime = System.currentTimeMillis();
    }


    //尝试将Block设置为封口
    /**
     * 中文：只尝试 OPEN 到 SEALED 的状态转换；不写 WAL 结束标记，也不自动提交上传或恢复。
     * English: Attempts only the OPEN-to-SEALED transition; does not write a WAL end marker or submit upload/recovery.
     * @return 是否由本次调用完成状态转换；English: whether this call performed the transition
     */
    public synchronized boolean trySeal() {
        return STATE_UPDATER.compareAndSet(this, OPEN, SEALED);
    }

    //尝试将Block设置为上传
    /**
     * 中文：在本对象锁下同时检查逻辑上传条件并领取 SEALED 到 UPLOADING；物理绑定和零引用仍由 core 同锁检查。
     * English: Checks logical eligibility and claims SEALED-to-UPLOADING under this monitor; core must additionally check physical binding and zero references under the same monitor.
     * @return 是否领取到本次上传资格；English: whether this call claimed upload admission
     */
    public synchronized boolean tryStartUpload() {
        return canUpload() && STATE_UPDATER.compareAndSet(this, SEALED, UPLOADING);
    }

    //尝试将Block设置上传成功
    /**
     * 中文：仅将 UPLOADING 原子改为 SUCCESS；调用者负责事先完成远端写入与 WAL 确认，此方法不执行 I/O 或通知。
     * English: Atomically changes only UPLOADING to SUCCESS; callers must first complete remote write and WAL acknowledgement, since this method performs neither I/O nor notification.
     * @return 是否成功转换状态；English: whether the state changed
     */
    public boolean markUploadSuccess() {
        return STATE_UPDATER.compareAndSet(this, UPLOADING, SUCCESS);
    }

    //尝试将Block设置为上传失败
    /**
     * 中文：将尚未终结的 OPEN/SEALED/UPLOADING 置为 FAILED，已成功或已失败则不变；不自动通知 Future。
     * English: Changes nonterminal OPEN/SEALED/UPLOADING to FAILED, leaving SUCCESS/FAILED unchanged; does not automatically notify Futures.
     * @return 是否首次转换为失败；English: whether this call newly transitioned to failure
     */
    public synchronized boolean markUploadFailed() {
        if (state == SUCCESS || state == FAILED) return false;
        STATE_UPDATER.set(this, FAILED);
        return true;
    }


    //尝试将Block设置为上传中，一般重试上传的时候会用
    /**
     * 中文：兼容重试入口，仅 CAS FAILED 到 UPLOADING，绕过 canUpload 校验且不提交任务；调用者必须自行重新保证资源与数据条件。
     * English: Compatibility retry entry performing only FAILED-to-UPLOADING CAS, bypassing canUpload and submitting no task; callers must reestablish resource and data preconditions.
     * @return 是否成功切换到上传中；English: whether transition to uploading succeeded
     */
    public boolean retryUpload() {
        return STATE_UPDATER.compareAndSet(this, FAILED, UPLOADING);
    }


    /**
     * 中文：原子增加已完成 WAL 映射复制的 value 字节计数，不触发刷盘。
     * English: Atomically adds completed WAL-mapping value bytes without forcing storage.
     * @param val 本次成功复制的 value 字节数，范围由调用者约束；English: value bytes copied successfully, validated by the caller
     */
    public void addPageCacheBytes(int val) {
        int current;
        int next;
        do {
            current = PAGE_CACHE_BYTES_UPDATER.get(this);
            next = current + val;
        } while (!PAGE_CACHE_BYTES_UPDATER.compareAndSet(this, current, next));
    }

    /**
     * 中文：原子增加 WAL 预留的 value 字节数；与封口互斥的完整预留操作仍须由调用者持有本对象锁。
     * English: Atomically adds reserved WAL value bytes; callers must still hold this monitor for the full reservation operation to exclude sealing.
     * @param val 预留的 value 字节数，范围由调用者约束；English: reserved value bytes, validated by the caller
     */
    public void addExpectedBytes(int val) {
        int current;
        int next;
        do {
            current = EXPECTED_BYTES_UPDATER.get(this);
            next = current + val;
        } while (!EXPECTED_BYTES_UPDATER.compareAndSet(this, current, next));
    }

    /**
     * 中文：原子累计物理块成功复制的 value 字节数，不更新 Future 位点或上传状态。
     * English: Atomically adds value bytes copied to the physical block without updating Future offsets or upload state.
     * @param val 本次复制成功的 value 字节数；English: value bytes successfully copied
     */
    public void addFinishedBytes(int val) {
        int current;
        int next;
        do {
            current = FINISHED_BYTES_UPDATER.get(this);
            next = current + val;
        } while (!FINISHED_BYTES_UPDATER.compareAndSet(this, current, next));
    }

    /**
     * 中文：直接清零物理完成字节，恢复方须先排除普通写入并取得独占恢复租约。
     * English: Directly resets physical-copy accounting; recovery must first exclude ordinary writers and acquire an exclusive recovery lease.
     */
    public void clearFinishedBytes() {
        this.finishedBytes = 0;
    }

    /**
     * 中文：在原始 core 请求的 finally 阶段累计已到达并结算字节，包括失败请求；此值不是成功复制量。
     * English: Accumulates settled bytes in each original core request's finally phase, including failures; this is not a successful-copy count.
     * @param bytes 当前原始请求的 value 字节数；English: value bytes in the original request
     */
    public synchronized void addCompletedAppendsBytes(int bytes) {
        completedAppendsBytes += bytes;
    }

    /**
     * 中文：在同一监视锁下读取原请求结算量，供恢复等待全部 WAL 到 core 请求到达。
     * English: Reads original-request settlement under the same monitor so recovery can await all WAL-to-core arrivals.
     * @return 已结算的 value 字节数；English: settled value bytes
     */
    public synchronized int getCompletedAppendsBytes() {
        return completedAppendsBytes;
    }

    /**
     * 中文：读取恢复状态快照，不自动取得恢复租约。
     * English: Reads recovery-state visibility without acquiring a recovery lease.
     * @return 是否处于完整块恢复中；English: whether full-block recovery is active
     */
    public boolean isRecovering() {
        return recovering;
    }

    /**
     * 中文：更新恢复状态位；调用方须在本对象锁下同时处理物理引用、游标和其他计数。
     * English: Updates the recovery flag; callers must coordinate physical references, cursors, and other counters under this monitor.
     * @param recovering 是否持有恢复阶段；English: whether the recovery phase is active
     */
    public void setRecovering(boolean recovering) {
        this.recovering = recovering;
    }

    /**
     * 中文：登记原请求上下文，重复 WAL 记录 ID 会覆盖旧条目；终态通知与晚到登记由上层状态协议协调。
     * English: Registers an original request context, replacing an existing entry with the same WAL record ID; upstream state coordination handles terminal notification and late registration.
     * @param future 非 null 的原请求上下文；English: non-null original request context
     */
    public synchronized void addFuture(FutureContext future) {
        futureMap.put(future.getWalRecordId(), future);
    }

    /**
     * 中文：按 WAL 记录 ID 查询原始上下文，供重放发布最终物理偏移；读取本身不冻结上下文。
     * English: Finds the original context by WAL record ID for replay to publish its final physical offset; lookup does not freeze the context.
     * @param walRecordId 块内 WAL 记录起始字节偏移；English: starting byte offset of the WAL record within its block
     * @return 已登记上下文或 null；English: registered context or null
     */
    public FutureContext getFuture(long walRecordId) {
        return futureMap.get(walRecordId);
    }

    /**
     * 中文：验证 SUCCESS 后提取当前登记结果并逐条异步派发；返回不意味着所有 Future 或用户回调已经执行。
     * English: After validating SUCCESS, drains registered results and dispatches each independently; return does not mean every Future or user callback has run.
     * @throws IllegalStateException 尚未处于 SUCCESS；English: primary state is not SUCCESS
     */
    public void completeAllFuture() {
        notifyFutures(true);
    }


    /**
     * 中文：提取当前登记项并派发 success=false 的普通 WriteResult，不以异常方式完成；调用者负责先确立失败或关闭语义。
     * English: Drains current registrations and dispatches ordinary WriteResults with success=false, not exceptional completion; callers first establish failure or shutdown semantics.
     */
    public void failAllFuture(){
        notifyFutures(false);
    }

    /**
     * 中文：每个 Future 独占一个通知虚拟线程，避免其同步回调阻塞上传关闭或同块其他 Future；不保证通知顺序，线程不属于上传执行器。
     * English: Uses one virtual notification thread per Future so synchronous continuations cannot block upload shutdown or sibling Futures; notification order is unspecified and these threads are not owned by the upload executor.
     * @param success 提取结果时使用的成功标志；English: success flag captured into results
     */
    private void notifyFutures(boolean success) {
        for (FutureNotification notification : drainNotifications(success)) {
            // complete 会同步执行用户的 thenAccept/whenComplete：用户可能关闭实例，
            // 也可能等待同一 Block 的另一条 Future，不能阻塞上传线程或串行通知循环。
            // 每条通知独立使用虚拟线程，且不属于上传执行器，避免 close 等待调用它的线程自身。
            // 中文：complete 的用户同步回调可能等待兄弟 Future 或调用 close，因此逐条独立派发且不加入上传执行器的关闭等待。
            // English: Synchronous complete continuations may await sibling Futures or call close, so notifications are independent and excluded from upload-executor shutdown waits.
            Thread.ofVirtual().name("cloudcache-future-notify").start(() ->
                    notification.future().complete(notification.result()));
        }
    }

    /**
     * 中文：在锁内快照不可变结果并清空本批登记，锁外才能运行用户回调；后续新增登记不在本批内，需由终态处理路径另行通知。
     * English: Snapshots immutable results and drains this batch under the monitor; user callbacks run outside it. Later registrations are outside this batch and require separate terminal-state handling.
     * @param success 是否生成成功结果，true 时必须已有 SUCCESS 状态；English: whether to produce successful results, requiring SUCCESS when true
     * @return 只携带堆内结果和原 Future 的通知快照；English: snapshots containing only heap results and original Futures
     * @throws IllegalStateException 成功结果请求早于 SUCCESS 状态；English: success requested before SUCCESS state
     */
    private synchronized List<FutureNotification> drainNotifications(boolean success) {
        if (success && state != SUCCESS) {
            throw new IllegalStateException("Block futures cannot succeed before S3 commit");
        }
        List<FutureNotification> notifications = new ArrayList<>(futureMap.size());
        for (FutureContext context : futureMap.values()) {
            // 在锁内复制最终结果；通知线程只持有普通堆内对象，不再读取可复用的 Block、
            // WAL 或堆外内存，因此实例释放这些资源不会影响稍后运行的通知与用户回调。
            // 中文：复制结果时仍持有本对象锁，异步通知不再访问会回收的 WAL 或物理块。
            // English: Capture results while holding this monitor so asynchronous notifications never revisit recyclable WAL or physical blocks.
            notifications.add(new FutureNotification(context.getFuture(), new WriteResult(
                    context.getS3Key(), context.getPhysicalOffset(), context.getSize(), success)));
        }
        futureMap.clear();
        return notifications;
    }

    /**
     * 中文：单条不可变通知快照；不持 WAL、可复用物理块或堆外内存，实例资源关闭后仍可安全发布已捕获结果。
     * English: Immutable notification snapshot without WAL, reusable-block, or native-memory references, allowing captured results to be published after instance resource closure.
     * @param future 原请求的 CompletableFuture；English: original request CompletableFuture
     * @param result 已捕获的不可变结果；English: captured immutable result
     */
    private record FutureNotification(CompletableFuture<WriteResult> future, WriteResult result) { }

    /**
     * 中文：暴露内部实时注册表而非副本；调用者不应绕过登记、快照和终态通知协议直接修改它。
     * English: Exposes the live internal registry rather than a copy; callers should not mutate it outside registration, snapshot, and terminal-notification protocols.
     * @return 内部并发映射；English: internal concurrent map
     */
    public ConcurrentHashMap<Long, FutureContext> getFutureMap() {
        return futureMap;
    }

    /**
     * 中文：记录当前 epoch 毫秒时间用于近似空闲判断；并发赋值允许小幅误差，不作单调时钟保证。
     * English: Records current epoch milliseconds for approximate idleness; concurrent assignments permit small deviations and do not guarantee monotonic time.
     */
    public void updateLastTime() {
        this.lastActiveTime = System.currentTimeMillis(); //这里可以容纳误差，简单赋值即可
    }

    /**
     * 中文：读取 WAL 映射复制计数快照，不是已强制刷盘字节数。
     * English: Reads a WAL-mapping copy-count snapshot, not forced-to-disk bytes.
     * @return 已复制的 value 字节数；English: copied value bytes
     */
    public int getPageCacheBytes() {
        return pageCacheBytes;
    }

    /**
     * 中文：读取物理块成功复制量快照。
     * English: Reads a snapshot of successful physical-block copy bytes.
     * @return 成功复制的 value 字节数；English: successfully copied value bytes
     */
    public int getFinishedBytes() {
        return finishedBytes;
    }

    /**
     * 中文：读取主状态快照，多条件判断仍需遵循同锁协议。
     * English: Reads primary state visibility; multi-condition decisions still require the shared-monitor protocol.
     * @return OPEN 至 FAILED 的状态常量；English: state constant from OPEN through FAILED
     */
    public int getState() {
        return state;
    }

    /**
     * 中文：读取已预留 value 总量快照。
     * English: Reads a snapshot of total reserved value bytes.
     * @return WAL 预留的 value 字节数；English: value bytes reserved in WAL
     */
    public int getExpectedBytes() {
        return expectedBytes;
    }

    /**
     * 中文：读取最近记录的活跃时间。
     * English: Reads the last recorded activity time.
     * @return epoch 毫秒值；English: epoch milliseconds
     */
    public long getLastActiveTime() {
        return lastActiveTime;
    }

    //将0位设置为1
    /**
     * 中文：在本对象锁下设置物理损坏位并保留恢复提交位；与封口协调，但不自行冻结块或入队。
     * English: Sets the physical-broken bit under this monitor while preserving the recovery-submitted bit; coordinates with sealing but does not freeze the block or enqueue work.
     */
    public void setBroken() {
        //setBroken和seal之间有冲突，因此加锁
        synchronized (this) {
            int pre = IS_BROKEN_UPDATER.get(this);
            IS_BROKEN_UPDATER.compareAndSet(this, pre, pre | 1);
        }
    }

    //将1位设置为1
    /**
     * 中文：标记已提交恢复候选，保留损坏位；实际入队由 manager 先执行。
     * English: Marks recovery-candidate submission while preserving broken state; the manager enqueues first.
     */
    public synchronized void setBrokenSubmit() {
        IS_BROKEN_UPDATER.set(this, isBroken | (1 << 1));
    }

    /**
     * 中文：读取物理损坏位。
     * English: Reads the physical-broken bit.
     * @return 位 0 是否为一；English: whether bit 0 is set
     */
    public boolean isBroken() {
        return (IS_BROKEN_UPDATER.get(this) & 1) == 1;
    }

    /**
     * 中文：读取恢复任务已提交位，不能据此推断恢复正在执行或已完成。
     * English: Reads the recovery-submitted bit, which does not establish that recovery is running or complete.
     * @return 位 1 是否为一；English: whether bit 1 is set
     */
    public boolean isBrokenSubmit() {
        return (IS_BROKEN_UPDATER.get(this) & (1 << 1)) == (1 << 1);
    }

    /**
     * 中文：同时清除损坏与恢复提交位；恢复方应先验证重放完整性，再允许上传。
     * English: Clears both broken and recovery-submitted bits; recovery must verify replay completeness before enabling upload.
     */
    public synchronized void setUnBroken() {
        IS_BROKEN_UPDATER.set(this, 0);
    }

    /**
     * 中文：读取损坏与恢复提交位的原始掩码。
     * English: Reads the raw broken/recovery-submitted bit mask.
     * @return 由位 0、1 组成的掩码，通常为 0 至 3；English: mask of bits 0 and 1, normally 0 through 3
     */
    public int getIsBroken() {
        return IS_BROKEN_UPDATER.get(this);
    }

    /**
     * 中文：检查已封口、非损坏、非恢复、正数 value 总量且 WAL/物理复制字节齐全；不检查物理引用数，不执行刷盘，也不要求原请求结算计数相等。
     * English: Checks sealed, unbroken, nonrecovering state and a positive value total equal to WAL and physical copy counts; does not check physical references, force storage, or require original-request settlement equality.
     * @return 当前逻辑数据是否符合上传条件；English: whether logical data is currently upload-eligible
     */
    public synchronized boolean canUpload() {
        return state == SEALED && !isBroken() && !recovering
                && expectedBytes > 0 && expectedBytes == pageCacheBytes && pageCacheBytes == finishedBytes;
    }
}
