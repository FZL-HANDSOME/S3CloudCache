package org.foreverfzl.cloudcache.core.cache;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class CacheBlockReferenceResource {
    protected final AtomicInteger refCount = new AtomicInteger(0); //目前有多少个线程准备写入
    private volatile boolean active = true; //是否可写入

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

}
