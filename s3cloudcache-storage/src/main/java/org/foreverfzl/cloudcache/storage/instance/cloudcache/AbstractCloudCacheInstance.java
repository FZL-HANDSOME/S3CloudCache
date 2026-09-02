package org.foreverfzl.cloudcache.storage.instance.cloudcache;

public abstract class AbstractCloudCacheInstance {

    public abstract void start();

    public abstract void close(long walWriteWaitTime, long blockWriteWaitTime, long upLoadWaitTime);
}
