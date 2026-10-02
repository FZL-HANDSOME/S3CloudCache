package org.foreverfzl.cloudcache.metadata.entity;


import org.foreverfzl.cloudchache.common.FutureContext;
import org.foreverfzl.cloudchache.common.WriteResult;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

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
    public static final int OPEN = 0;
    public static final int SEALED = 1;
    public static final int UPLOADING = 2;
    public static final int SUCCESS = 3;
    public static final int FAILED = 4;

    private static final AtomicIntegerFieldUpdater<BlockMetaData> STATE_UPDATER;
    private volatile int state;
    //expectedBytes代表该逻辑block期望的字节数
    private volatile int expectedBytes;
    private static final AtomicIntegerFieldUpdater<BlockMetaData> EXPECTED_BYTES_UPDATER;
    //pageCacheBytes代表该逻辑block写入到操作系统PageCache的字节数
    private volatile int pageCacheBytes;
    private static final AtomicIntegerFieldUpdater<BlockMetaData> PAGE_CACHE_BYTES_UPDATER;
    //finishedBytes代表物理Block真正写入字节数
    private volatile int finishedBytes;
    private static final AtomicIntegerFieldUpdater<BlockMetaData> FINISHED_BYTES_UPDATER;
    private volatile long lastActiveTime;
    // 原请求走完 WAL -> Core 后才计入，包含物理写入失败；恢复不能越过尚未到达 Core 的请求。
    private int completedAppendsBytes;
    private volatile boolean recovering;

    //该isBroken指的是物理block
    protected static final AtomicIntegerFieldUpdater<BlockMetaData> IS_BROKEN_UPDATER;
    //二进制：第0位为 1则代表broken，第1位为1则代表已经将任务上传到队列中了
    private volatile int isBroken = 0;

    //专门存放该block对应的future
    ConcurrentHashMap<Long, FutureContext> futureMap = new ConcurrentHashMap<>();

    static {
        STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "state");
        EXPECTED_BYTES_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "expectedBytes");
        PAGE_CACHE_BYTES_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "pageCacheBytes");
        FINISHED_BYTES_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "finishedBytes");
        IS_BROKEN_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BlockMetaData.class, "isBroken");
    }

    public BlockMetaData() {
        this.state = 0;
        this.expectedBytes = 0;
        this.finishedBytes = 0;
        this.lastActiveTime = System.currentTimeMillis();
    }


    //尝试将Block设置为封口
    public synchronized boolean trySeal() {
        return STATE_UPDATER.compareAndSet(this, OPEN, SEALED);
    }

    //尝试将Block设置为上传
    public synchronized boolean tryStartUpload() {
        return canUpload() && STATE_UPDATER.compareAndSet(this, SEALED, UPLOADING);
    }

    //尝试将Block设置上传成功
    public boolean markUploadSuccess() {
        return STATE_UPDATER.compareAndSet(this, UPLOADING, SUCCESS);
    }

    //尝试将Block设置为上传失败
    public synchronized boolean markUploadFailed() {
        if (state == SUCCESS || state == FAILED) return false;
        STATE_UPDATER.set(this, FAILED);
        return true;
    }


    //尝试将Block设置为上传中，一般重试上传的时候会用
    public boolean retryUpload() {
        return STATE_UPDATER.compareAndSet(this, FAILED, UPLOADING);
    }


    public void addPageCacheBytes(int val) {
        int current;
        int next;
        do {
            current = PAGE_CACHE_BYTES_UPDATER.get(this);
            next = current + val;
        } while (!PAGE_CACHE_BYTES_UPDATER.compareAndSet(this, current, next));
    }

    public void addExpectedBytes(int val) {
        int current;
        int next;
        do {
            current = EXPECTED_BYTES_UPDATER.get(this);
            next = current + val;
        } while (!EXPECTED_BYTES_UPDATER.compareAndSet(this, current, next));
    }

    public void addFinishedBytes(int val) {
        int current;
        int next;
        do {
            current = FINISHED_BYTES_UPDATER.get(this);
            next = current + val;
        } while (!FINISHED_BYTES_UPDATER.compareAndSet(this, current, next));
    }

    public void clearFinishedBytes() {
        this.finishedBytes = 0;
    }

    public synchronized void addCompletedAppendsBytes(int bytes) {
        completedAppendsBytes += bytes;
    }

    public synchronized int getCompletedAppendsBytes() {
        return completedAppendsBytes;
    }

    public boolean isRecovering() {
        return recovering;
    }

    public void setRecovering(boolean recovering) {
        this.recovering = recovering;
    }

    public synchronized void addFuture(FutureContext future) {
        futureMap.put(future.getWalRecordId(), future);
    }

    public FutureContext getFuture(long walRecordId) {
        return futureMap.get(walRecordId);
    }

    public void completeAllFuture() {
        notifyFutures(true);
    }


    public void failAllFuture(){
        notifyFutures(false);
    }

    private void notifyFutures(boolean success) {
        for (FutureNotification notification : drainNotifications(success)) {
            // complete 会同步执行用户的 thenAccept/whenComplete：用户可能关闭实例，
            // 也可能等待同一 Block 的另一条 Future，不能阻塞上传线程或串行通知循环。
            // 每条通知独立使用虚拟线程，且不属于上传执行器，避免 close 等待调用它的线程自身。
            Thread.ofVirtual().name("cloudcache-future-notify").start(() ->
                    notification.future().complete(notification.result()));
        }
    }

    private synchronized List<FutureNotification> drainNotifications(boolean success) {
        if (success && state != SUCCESS) {
            throw new IllegalStateException("Block futures cannot succeed before S3 commit");
        }
        List<FutureNotification> notifications = new ArrayList<>(futureMap.size());
        for (FutureContext context : futureMap.values()) {
            // 在锁内复制最终结果；通知线程只持有普通堆内对象，不再读取可复用的 Block、
            // WAL 或堆外内存，因此实例释放这些资源不会影响稍后运行的通知与用户回调。
            notifications.add(new FutureNotification(context.getFuture(), new WriteResult(
                    context.getS3Key(), context.getPhysicalOffset(), context.getSize(), success)));
        }
        futureMap.clear();
        return notifications;
    }

    private record FutureNotification(CompletableFuture<WriteResult> future, WriteResult result) { }

    public ConcurrentHashMap<Long, FutureContext> getFutureMap() {
        return futureMap;
    }

    public void updateLastTime() {
        this.lastActiveTime = System.currentTimeMillis(); //这里可以容纳误差，简单赋值即可
    }

    public int getPageCacheBytes() {
        return pageCacheBytes;
    }

    public int getFinishedBytes() {
        return finishedBytes;
    }

    public int getState() {
        return state;
    }

    public int getExpectedBytes() {
        return expectedBytes;
    }

    public long getLastActiveTime() {
        return lastActiveTime;
    }

    //将0位设置为1
    public void setBroken() {
        //setBroken和seal之间有冲突，因此加锁
        synchronized (this) {
            int pre = IS_BROKEN_UPDATER.get(this);
            IS_BROKEN_UPDATER.compareAndSet(this, pre, pre | 1);
        }
    }

    //将1位设置为1
    public synchronized void setBrokenSubmit() {
        IS_BROKEN_UPDATER.set(this, isBroken | (1 << 1));
    }

    public boolean isBroken() {
        return (IS_BROKEN_UPDATER.get(this) & 1) == 1;
    }

    public boolean isBrokenSubmit() {
        return (IS_BROKEN_UPDATER.get(this) & (1 << 1)) == (1 << 1);
    }

    public synchronized void setUnBroken() {
        IS_BROKEN_UPDATER.set(this, 0);
    }

    public int getIsBroken() {
        return IS_BROKEN_UPDATER.get(this);
    }

    public synchronized boolean canUpload() {
        return state == SEALED && !isBroken() && !recovering
                && expectedBytes > 0 && expectedBytes == pageCacheBytes && pageCacheBytes == finishedBytes;
    }
}
