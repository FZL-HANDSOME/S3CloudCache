package org.foreverfzl.cloudcache.storage.instance;

/**
 * 我们设计的是一个Bucket的S3key只能有一个前缀，因此设计一个对象可以避免一个Bucket弄多个prefix
 */
public class BucketInfo {
    public final String bucketName;
    public final String prefix;

    public BucketInfo(String bucketName, String prefix) {
        this.bucketName = bucketName;
        this.prefix = prefix;
    }
}
