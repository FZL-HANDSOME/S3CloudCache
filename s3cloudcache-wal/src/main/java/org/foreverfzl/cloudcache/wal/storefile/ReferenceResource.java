package org.foreverfzl.cloudcache.wal.storefile;


import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {

    protected final AtomicLong refCount = new AtomicLong(0); //目前又多少个线程使用我
    protected volatile boolean available = true; //是否可引用
    protected volatile boolean cleanup = false; //是否要删除这个文件
    protected volatile boolean cleanupOver = false;


    public boolean isAvailable() {
        return this.available;
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    //关闭该文件，让该文件不写入
    public void delete() {
        if(refCount.get()!=0){
            //有其他的线程引用
            this.available = false;
            cleanup = true;
        }else {
            //没有其它线程引用直接删除
            synchronized (this){
                cleanupOver=realClean();
            }
        }

    }

    public void release() {
        refCount.decrementAndGet();
        if(cleanup){
            synchronized (this) {
                if(!cleanupOver){
                    cleanupOver=realClean();
                }
            }
        }
    }

    public abstract boolean realClean();

    public void hold() {
        if (!available) {
            refCount.incrementAndGet();
        }
    }


}
