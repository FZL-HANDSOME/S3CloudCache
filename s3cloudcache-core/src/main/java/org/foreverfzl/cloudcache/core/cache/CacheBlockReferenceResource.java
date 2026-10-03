package org.foreverfzl.cloudcache.core.cache;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 中文：提供引用计数与写入/延迟回收标志；单独调用标志 setter 不构成原子租约，调用方须遵守具体 Block 的元数据锁协议。
 * English: Provides reference and admission/recycling flags; individual setters do not acquire a lease, and callers must follow the concrete block's metadata-lock protocol.
 */
public abstract class CacheBlockReferenceResource {
    /**
     * 中文：当前写入、上传和整块恢复持有者数量；0 不代表 WAL 已提交。
     * English: Counts writer, upload and whole-block recovery holders; zero does not imply WAL commitment.
     */
    protected final AtomicInteger refCount = new AtomicInteger(0); //目前有多少个线程横在写入和上传
    /**
     * 中文：是否允许物理写入；仅表示准入标志，不等价于逻辑块 OPEN 状态。
     * English: Physical-write admission flag; it is not equivalent to the logical OPEN state.
     */
    private volatile boolean active = true; //是否可写入
    //只有当需要延迟清除回收block的时候才会将isClean设置为true
    //上传成功、失败后立刻清除回收block有对应的方法 不会修改该变量
    /**
     * 中文：补充更正：当前上传成功/失败也会设置此标志；真正回池须等待最后引用释放。
     * English: Correction: current upload success/failure also sets this flag; pool return waits for the final reference release.
     */
    private volatile boolean isDelayClean = false;


    /**
     * 中文：释放一次持有；最后引用的收尾由子类决定，必须与获取严格配对。
     * English: Releases one hold; subclasses handle final-release settlement, and every acquisition must have a matching release.
     */
    public abstract void releaseReference();

    /**
     * 中文：获取一次持有；此抽象接口本身不检查 active 或逻辑身份。
     * English: Acquires one hold; this abstract contract does not itself validate active state or logical identity.
     */
    public abstract void getReference();

    /**
     * 中文：读取当前持有者数量快照，不阻止并发变化。
     * English: Reads a holder-count snapshot without preventing concurrent changes.
     * @return 当前引用数；English: Current reference count.
     */
    public int getCurReferenceCount() {
        return refCount.get();
    }

    /**
     * 中文：允许后续物理写入；调用方须先确认绑定或恢复租约正确。
     * English: Allows subsequent physical writes after the caller has validated binding or recovery ownership.
     */
    public void setActive() {
        this.active = true;
    }

    /**
     * 中文：禁止后续通过准入检查的写入；不会取消已持有引用的复制。
     * English: Disables subsequent admitted writes; it does not cancel copies already holding a reference.
     */
    public void setUnActive() {
        this.active = false;
    }

    /**
     * 中文：读取物理写入准入标志。
     * English: Reads the physical-write admission flag.
     * @return 当前是否 active；English: Whether the active flag is set.
     */
    public boolean isActive() {
        return active;
    }

    /**
     * 中文：请求延迟回收；不会自行清除数据或加入空闲池。
     * English: Requests deferred recycling without clearing data or enqueuing the block itself.
     */
    public void setDelayClean() {
        this.isDelayClean = true;
    }

    /**
     * 中文：清除延迟回收标志，通常由已无引用的池重置流程调用。
     * English: Clears deferred recycling, normally during an unreferenced pool reset.
     */
    public void setUnDelayClean() {
        this.isDelayClean = false;
    }

    /**
     * 中文：读取是否已请求延迟回收。
     * English: Reads whether deferred recycling was requested.
     * @return 延迟回收标志；English: Deferred-recycling flag.
     */
    public boolean isDelayClean() {
        return isDelayClean;
    }

}
