package org.foreverfzl.cloudcache.core.cache;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public abstract class CacheBlockReferenceResource {
    protected final AtomicInteger refCount = new AtomicInteger(0); //目前又多少个线程使用我
    public abstract void releaseReference();
    public abstract void getReference();
}
