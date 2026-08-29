package org.foreverfzl.cloudcache.core.cache;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class CacheBlockReferenceResource {
    protected final AtomicInteger refCount = new AtomicInteger(0); //目前有多少个线程横在写入和上传
    private volatile boolean active = true; //是否可写入
    //只有当需要延迟清除回收block的时候才会将isClean设置为true
    //上传成功、失败后立刻清除回收block有对应的方法 不会修改该变量
    private volatile boolean isDelayClean = false;


    public abstract void releaseReference();

    public abstract void getReference();

    public int getCurReferenceCount() {
        return refCount.get();
    }

    public void setActive() {
        this.active = true;
    }

    public void setUnActive() {
        this.active = false;
    }

    public boolean isActive() {
        return active;
    }

    public void setDelayClean() {
        this.isDelayClean = true;
    }

    public void setUnDelayClean() {
        this.isDelayClean = false;
    }
    public boolean isDelayClean() {
        return isDelayClean;
    }

}
