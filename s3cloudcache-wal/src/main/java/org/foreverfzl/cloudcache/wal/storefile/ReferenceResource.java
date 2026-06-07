package org.foreverfzl.cloudcache.wal.storefile;


import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {

    protected final AtomicInteger refCount = new AtomicInteger(0); //目前又多少个线程使用我
    protected volatile boolean available = true; //是否可引用
    protected volatile boolean cleanup = false; //是否要删除这个文件


    public boolean isAvailable() {
        return this.available;
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    //关闭文件
    public void close() {
        this.available = false;
    }

    public void delete() {
        this.cleanup = true;
    }

    //释放该文件
    public int release() {
        return refCount.decrementAndGet();
    }


    //获取该文件
    public int hold() {
        if (!available) {
            return refCount.incrementAndGet();
        }
        return -1;
    }


}
