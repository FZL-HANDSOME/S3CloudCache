package org.foreverfzl.cloudchache.common.config;

import java.util.HashMap;
import java.util.Map;

/**
 * 该类存储真个项目的配置
 *
 */

/**
 * 配置文件样式
 * s3-cloud-cache:
 * # Instance 级别的全局物理资源限制
 * global-max-memory: 10GB
 * <p>
 * # 默认的 Bucket 配置（作为全量基线，字段必须完整）
 * default-bucket:
 * wal-size: 268435456       # 默认 256MB
 * cache-size: 67108864      # 默认 64MB
 * flush-interval-ms: 1000   # 默认 1000ms
 * <p>
 * # 特殊的 Bucket 配置列表（未配置的字段，运行时自动继承 default-bucket）
 * special-buckets:
 * coupon-bucket:
 * cache-size: 536870912   # 仅覆盖 Cache 大小为 512MB，WAL 和刷盘时间继承默认值
 * audit-log-bucket:
 * wal-size: 1073741824    # 仅覆盖 WAL 大小为 1GB
 * cache-size: 4194304     # 仅覆盖 Cache 大小为 4MB
 *
 */
public class S3CloudCacheConfig {


    public String instanceName = null;
    /**
     * 用户指定的持久化目录
     */
    public String walPath = null;

    /**
     * 一个Block最大的空闲时间，如果一个Block M毫秒内没有新的数据写入，自动封口上传数据(可以理解为一个Bucket的最大空闲时间，超过这个时间自动上传)
     */
    public Integer blockMaxIdleTime = 20000;

    /**
     * 默认配置文件
     */
    public BucketConfig defaultBucketConfig;
    /**
     * 特殊配置文件
     */
    public Map<String, BucketConfig> specialBuckets = new HashMap<>();

    public S3CloudCacheConfig(String instanceName, String walPath, BucketConfig defaultBucketConfig) {
        this.instanceName = instanceName;
        this.walPath = walPath;
        this.defaultBucketConfig = defaultBucketConfig;
    }

    public S3CloudCacheConfig(String instanceName, String walPath, BucketConfig defaultBucketConfig, Map<String, BucketConfig> specialBuckets) {
        this.instanceName = instanceName;
        this.walPath = walPath;
        this.defaultBucketConfig = defaultBucketConfig;
        this.specialBuckets = specialBuckets;
    }

    public S3CloudCacheConfig setInstanceName(String instanceName) {
        this.instanceName = instanceName;
        return this;
    }

    public S3CloudCacheConfig setWalPath(String walPath) {
        this.walPath = walPath;
        return this;
    }

    public S3CloudCacheConfig setDefaultBucketConfig(BucketConfig defaultBucketConfig) {
        this.defaultBucketConfig = defaultBucketConfig;
        return this;
    }

    public S3CloudCacheConfig setSpecialBucket(String bucketName, BucketConfig config) {
        specialBuckets.put(bucketName, config);
        return this;
    }

    public BucketConfig getBucketConfig(String bucketName) {
        BucketConfig bucketConfig = specialBuckets.get(bucketName);
        if (bucketConfig != null) return bucketConfig;
        return defaultBucketConfig;
    }

}

