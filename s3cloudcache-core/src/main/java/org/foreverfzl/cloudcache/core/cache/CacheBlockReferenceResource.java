package org.foreverfzl.cloudcache.core.cache;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public abstract class CacheBlockReferenceResource {
    protected final AtomicInteger refCount = new AtomicInteger(0); //目前又多少个线程使用我
    private volatile boolean active = true;

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
