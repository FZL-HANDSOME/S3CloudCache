package org.foreverfzl.cloudcache.wal.storefile;


import java.util.concurrent.atomic.AtomicInteger;

public abstract class MappedFiledReferenceResource {

    private final AtomicInteger refCount = new AtomicInteger(0); //目前又多少个线程使用我
    private volatile boolean available = true; //是否可写
    private volatile boolean cleanup = false; //是否要删除这个文件


    public boolean isAvailable() {
        return this.available;
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    //关闭文件写入
    public void close() {
        if(available) this.available = false;
    }

    public void delete() {
        this.available=false;
        this.cleanup = true;
    }

    //释放该文件的引用
    public int release() {
        return refCount.decrementAndGet();
    }


    //获取该文件的引用
    public int hold() {
        if (!available) {
            return refCount.incrementAndGet();
        }
        return -1;
    }


}
