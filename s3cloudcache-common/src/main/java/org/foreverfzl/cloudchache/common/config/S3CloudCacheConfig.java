package org.foreverfzl.cloudchache.common.config;

import java.util.HashMap;
import java.util.Map;
import java.nio.file.Path;

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

    /**
     * 在实例启动前校验配置并深拷贝每个 Bucket，避免调用方修改配置后导致现存 Block 与新 WAL 尺寸不一致。
     * 本方法只检查配置，不创建目录、线程或内存映射；配置修改应通过关闭后重新创建实例生效。
     */
    public S3CloudCacheConfig snapshotAndValidate() {
        String name = this.instanceName;
        String directory = this.walPath;
        Integer maxIdleTime = this.blockMaxIdleTime;
        validatePathComponent(name, "instanceName");
        if (directory != null) {
            if (directory.isBlank()) throw new IllegalArgumentException("walPath must not be blank");
            Path.of(directory); // 提前拒绝本平台无法表示的路径，但不要求目录已经存在。
        }
        if (maxIdleTime == null || maxIdleTime <= 0) {
            throw new IllegalArgumentException("blockMaxIdleTime must be positive");
        }
        if (defaultBucketConfig == null) throw new IllegalArgumentException("defaultBucketConfig must not be null");
        if (specialBuckets == null) throw new IllegalArgumentException("specialBuckets must not be null");
        BucketConfig defaultCopy = defaultBucketConfig.copyAndValidate();
        Map<String, BucketConfig> bucketCopies = new HashMap<>();
        // 特殊 Bucket 仍是完整配置，不在此隐式改变现有配置覆盖规则。
        for (Map.Entry<String, BucketConfig> entry : specialBuckets.entrySet()) {
            validatePathComponent(entry.getKey(), "special bucket name");
            if (entry.getValue() == null) throw new IllegalArgumentException("special bucket config must not be null");
            bucketCopies.put(entry.getKey(), entry.getValue().copyAndValidate());
        }
        S3CloudCacheConfig snapshot = new S3CloudCacheConfig(name, directory, defaultCopy, bucketCopies);
        snapshot.blockMaxIdleTime = maxIdleTime;
        return snapshot;
    }

    /**
     * 实例名和 Bucket 名都是 WAL 根目录下的单个目录分量，不能携带路径跳转或 Windows 驱动器标记。
     * 不施加完整 S3 命名规则，保留中文、下划线等原有可用名字。
     */
    public static void validatePathComponent(String value, String name) {
        if (value == null || value.isBlank() || value.equals(".") || value.equals("..")
                || value.indexOf('/') >= 0 || value.indexOf('\\') >= 0
                || value.indexOf(':') >= 0 || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " must be a single nonempty path component");
        }
        Path path = Path.of(value);
        if (path.isAbsolute() || path.getNameCount() != 1) {
            throw new IllegalArgumentException(name + " must be a single relative path component");
        }
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

