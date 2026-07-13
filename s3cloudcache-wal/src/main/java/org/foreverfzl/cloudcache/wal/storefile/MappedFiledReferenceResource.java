package org.foreverfzl.cloudcache.wal.storefile;


import java.util.concurrent.atomic.AtomicInteger;

public abstract class MappedFiledReferenceResource {

    private final AtomicInteger refCount = new AtomicInteger(0); //目前又多少个线程使用我
    private volatile boolean available = true; //是否可写
    private volatile boolean cleanup = false; //文件资源是否清除


    public boolean isAvailable() {
        return this.available;
    }

    public int getRefCount() {
        return this.refCount.get();
    }

    //关闭文件写入
    public void close() {
        if(available) this.available = false;
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

    public boolean isCleanup() {
        return cleanup;
    }

}
