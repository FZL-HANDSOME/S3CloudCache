package org.foreverfzl.cloudcache.wal.storefile;


import java.util.concurrent.atomic.AtomicInteger;

/**
 * 中文：提供引用数、可写状态与清理标志的独立账本；hold/release 不检查生命周期，也不自动释放文件。调用方必须保证配对及清理前无使用者。
 * English: Tracks references, write availability, and cleanup independently. hold/release neither validate lifecycle nor release files; callers must pair them and exclude users before cleanup.
 */
public abstract class MappedFiledReferenceResource {

    /**
     * 中文：已登记的持有次数，初值为0；不是文件是否完全上传的指标。
     * English: Registered holds, initially zero; this does not indicate upload completion.
     */
    private final AtomicInteger refCount = new AtomicInteger(0); //目前又多少个线程使用我
    /**
     * 中文：写入准入标志；false不表示映射已卸载。
     * English: Write-admission flag; false does not mean the mapping is unmapped.
     */
    private volatile boolean available = true; //是否可写
    /**
     * 中文：清理流程已经标记开始；该标志自身不能证明所有清理操作成功。
     * English: Marks that cleanup has been entered; the flag alone does not prove every cleanup operation succeeded.
     */
    private volatile boolean cleanup = false; //文件资源是否清除，为true也代表该文件可以被删除


    /**
     * 中文：读取写入准入标志，不获取引用。
     * English: Reads write availability without acquiring a reference.
     *
     * @return 中文：当前是否允许新写入；English: whether new writes are currently permitted
     */
    public boolean isAvailable() {
        return this.available;
    }

    /**
     * 中文：读取瞬时引用数；用于判断时仍需要外部生命周期协调。
     * English: Reads a reference-count snapshot; lifecycle decisions still require external coordination.
     *
     * @return 中文：当前持有次数；English: current hold count
     */
    public int getRefCount() {
        return this.refCount.get();
    }

    //关闭文件写入
    /**
     * 中文：禁止后续写入；不会 force、关闭 Channel、卸载 Arena 或删除文件。
     * English: Disables subsequent writes without forcing data, closing the channel, unmapping the arena, or deleting the file.
     */
    public void close() {
        if (available) this.available = false;
    }

    //释放该文件的引用
    /**
     * 中文：仅将引用数减一，不触发自动清理；多释放会产生负数，必须由调用方避免。
     * English: Only decrements references; it does not trigger cleanup. Callers must prevent unmatched releases and negative counts.
     *
     * @return 中文：释放后的引用数；English: reference count after release
     */
    public int release() {
        return refCount.decrementAndGet();
    }

    //获取该文件的引用
    /**
     * 中文：仅将引用数加一；即使已关闭也不拒绝，因此不是原子的可用性租约。
     * English: Only increments references and does not reject closed resources; it is not an atomic availability lease.
     *
     * @return 中文：获取后的引用数；English: reference count after acquisition
     */
    public int hold() {
        return refCount.incrementAndGet();
    }

    /**
     * 中文：设置清理标志；实际资源释放由具体文件类执行。
     * English: Sets the cleanup flag; the concrete file implementation releases resources.
     */
    public void setClean() {
        if (!cleanup) this.cleanup = true;
    }

    /**
     * 中文：读取清理标志，不执行清理。
     * English: Reads the cleanup flag without performing cleanup.
     *
     * @return 中文：是否已被标记清理；English: whether cleanup has been marked
     */
    public boolean isCleanup() {
        return cleanup;
    }

}
