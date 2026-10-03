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
/**
 * 中文：实例级可变配置输入，连接默认 Bucket 配置及按名称选择的特殊配置；实例启动使用独立深快照。
 * English: Mutable instance configuration combining default and named Bucket settings; startup uses an independent deep snapshot.
 * 中文：构造器和 setter 保留传入引用，不负责线程安全；调用方应在实例构造前完成配置组装。
 * English: Constructors and setters retain supplied references without thread safety; callers should finish configuration before instance construction.
 */
public class S3CloudCacheConfig {


    /**
     * 中文：本地实例目录名及 S3 Key 身份的一部分；初值 null，启动快照要求非空的单个相对路径分量。
     * English: Local instance directory name and part of S3 key identity; initially null, requiring one nonempty relative path component at startup.
     */
    public String instanceName = null;
    /**
     * 用户指定的持久化目录
     * 中文：调用方提供的基础目录，默认 null 表示使用 user.home；实例在其后拼接 CloudCache/store 和实例名。
     * English: Caller-supplied base directory, default null meaning user.home; the instance appends CloudCache/store and the instance name.
     * 中文：非 null 时必须可被本平台解析且非空白；快照校验不创建目录，也不检查磁盘权限。
     * English: Non-null values must be nonblank and parseable on the current platform; snapshot validation creates no directory and checks no disk permissions.
     */
    public String walPath = null;

    /**
     * 一个Block最大的空闲时间，如果一个Block M毫秒内没有新的数据写入，自动封口上传数据(可以理解为一个Bucket的最大空闲时间，超过这个时间自动上传)
     * 中文：空闲封口阈值，单位毫秒，默认 20000，必须为正；后台检查触发，不承诺精确到期即完成上传。
     * English: Idle-sealing threshold in milliseconds, default 20000 and strictly positive; background checks do not guarantee upload completion at the exact deadline.
     */
    public Integer blockMaxIdleTime = 20000;

    /**
     * 默认配置文件
     * 中文：未匹配特殊名称时使用的完整配置；必须非 null，实例快照会复制它而不是共享调用方对象。
     * English: Complete fallback configuration for unmatched names; must be non-null and is copied by the instance snapshot.
     */
    public BucketConfig defaultBucketConfig;
    /**
     * 特殊配置文件
     * 中文：Bucket 名到完整配置的映射，默认空 HashMap；命中时直接选中对应对象，快照会复制 Map 和所有值。
     * English: Bucket-name to complete-configuration map, initially an empty HashMap; lookup selects the matching object, and snapshots copy the map and every value.
     */
    public Map<String, BucketConfig> specialBuckets = new HashMap<>();

    /**
     * 中文：创建仅包含默认 Bucket 配置的输入对象，保留传入引用，特殊配置表初始为空。
     * English: Creates an input object with default Bucket settings, retaining supplied references and an initially empty override map.
     * @param instanceName 中文：实例目录及键身份名称；English: instance directory and key identity name
     * @param walPath 中文：基础 WAL 目录，null 使用用户目录；English: base WAL directory, null for the user directory
     * @param defaultBucketConfig 中文：默认完整 Bucket 配置；English: complete default Bucket configuration
     */
    public S3CloudCacheConfig(String instanceName, String walPath, BucketConfig defaultBucketConfig) {
        this.instanceName = instanceName;
        this.walPath = walPath;
        this.defaultBucketConfig = defaultBucketConfig;
    }

    /**
     * 中文：保存默认及特殊配置引用；此处不复制 Map，不合并单个字段，也不启动资源。
     * English: Retains default and named configuration references without copying the map, merging fields, or starting resources.
     * @param instanceName 中文：实例目录及键身份名称；English: instance directory and key identity name
     * @param walPath 中文：基础 WAL 目录，null 使用用户目录；English: base WAL directory, null for the user directory
     * @param defaultBucketConfig 中文：未匹配时使用的完整配置；English: complete fallback configuration
     * @param specialBuckets 中文：名称到完整特殊配置的映射；English: name-to-complete-configuration map
     */
    public S3CloudCacheConfig(String instanceName, String walPath, BucketConfig defaultBucketConfig, Map<String, BucketConfig> specialBuckets) {
        this.instanceName = instanceName;
        this.walPath = walPath;
        this.defaultBucketConfig = defaultBucketConfig;
        this.specialBuckets = specialBuckets;
    }

    /**
     * 在实例启动前校验配置并深拷贝每个 Bucket，避免调用方修改配置后导致现存 Block 与新 WAL 尺寸不一致。
     * 本方法只检查配置，不创建目录、线程或内存映射；配置修改应通过关闭后重新创建实例生效。
     * 中文：返回对象仍可变，但其默认配置、Map 和特殊配置均不与源对象共享；复制期间应避免并发修改源配置。
     * English: The result remains mutable, but its default configuration, map, and named configurations are independent; avoid concurrent source mutation while copying.
     * @return 中文：实例可持有的已验证配置快照；English: validated configuration snapshot for the instance
     * @throws IllegalArgumentException 中文：名称、路径、时间或任意 Bucket 配置不合法；English: invalid name, path, timing, or Bucket settings
     */
    public S3CloudCacheConfig snapshotAndValidate() {
        String name = this.instanceName;
        String directory = this.walPath;
        Integer maxIdleTime = this.blockMaxIdleTime;
        validatePathComponent(name, "instanceName");
        // 中文：null 保留默认目录语义；非 null 只做语法验证，不在配置阶段创建目录或锁文件。
        // English: Null preserves the default-directory behavior; non-null paths are syntax-checked without creating directories or locks.
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
        // 中文：逐项复制并验证，避免尚未创建 Writer 的特殊配置绕过启动前校验。
        // English: Copy and validate every entry so settings for lazily created Writers cannot bypass startup checks.
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
     * 中文：统一拒绝两个平台的分隔符和 Windows 驱动器/流标记，再交给当前平台 Path 做语法检查。
     * English: Rejects both separator styles and Windows drive/stream markers before applying current-platform Path syntax checks.
     * @param value 中文：待验证的实例或 Bucket 名称；English: instance or Bucket name to validate
     * @param name 中文：异常消息使用的配置项名称；English: setting name used in diagnostics
     * @throws IllegalArgumentException 中文：值为空、含路径跳转或不能作为单个相对路径分量；English: empty, traversing, or invalid single relative path component
     */
    public static void validatePathComponent(String value, String name) {
        if (value == null || value.isBlank() || value.equals(".") || value.equals("..")
                || value.indexOf('/') >= 0 || value.indexOf('\\') >= 0
                || value.indexOf(':') >= 0 || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " must be a single nonempty path component");
        }
        // 中文：字符白名单之外仍需检查平台路径语法；不解析真实路径，也不在此解决符号链接。
        // English: Platform path syntax still needs checking after character checks; this does not resolve filesystem paths or symbolic links.
        Path path = Path.of(value);
        if (path.isAbsolute() || path.getNameCount() != 1) {
            throw new IllegalArgumentException(name + " must be a single relative path component");
        }
    }

    /**
     * 中文：更新输入配置的实例名，合法性在快照时检查，不重命名已创建的实例目录。
     * English: Updates the input instance name for validation at snapshot time without renaming an existing instance directory.
     * @param instanceName 中文：实例名称；English: instance name
     * @return 中文：当前配置对象；English: this configuration
     */
    public S3CloudCacheConfig setInstanceName(String instanceName) {
        this.instanceName = instanceName;
        return this;
    }

    /**
     * 中文：更新基础目录输入，不移动已存在 WAL，也不立即创建目录。
     * English: Updates the base-directory input without moving existing WAL or creating directories immediately.
     * @param walPath 中文：基础目录，null 使用 user.home；English: base directory, null for user.home
     * @return 中文：当前配置对象；English: this configuration
     */
    public S3CloudCacheConfig setWalPath(String walPath) {
        this.walPath = walPath;
        return this;
    }

    /**
     * 中文：保留新的默认配置引用；实例快照时才进行复制和完整校验。
     * English: Retains a new default-configuration reference; copying and full validation occur when taking the instance snapshot.
     * @param defaultBucketConfig 中文：默认完整配置；English: complete default configuration
     * @return 中文：当前配置对象；English: this configuration
     */
    public S3CloudCacheConfig setDefaultBucketConfig(BucketConfig defaultBucketConfig) {
        this.defaultBucketConfig = defaultBucketConfig;
        return this;
    }

    /**
     * 中文：为名称注册或替换完整 Bucket 配置；不复制配置，也不按字段与默认配置合并。
     * English: Registers or replaces a complete named Bucket configuration without copying it or merging fields with defaults.
     * @param bucketName 中文：待配置的 Bucket 名称；English: Bucket name to configure
     * @param config 中文：对应完整配置引用；English: corresponding complete configuration reference
     * @return 中文：当前配置对象；English: this configuration
     * @throws NullPointerException 中文：specialBuckets 已被外部设置为 null；English: specialBuckets has been set to null externally
     */
    public S3CloudCacheConfig setSpecialBucket(String bucketName, BucketConfig config) {
        specialBuckets.put(bucketName, config);
        return this;
    }

    /**
     * 中文：优先返回名称匹配的非 null 特殊配置，否则返回默认对象；返回的是内部引用而非新快照。
     * English: Returns a non-null named configuration when present, otherwise the default; the result is an internal reference, not a new snapshot.
     * @param bucketName 中文：查询的 Bucket 名称；English: Bucket name to look up
     * @return 中文：选中的完整 BucketConfig 引用；English: selected complete BucketConfig reference
     * @throws NullPointerException 中文：specialBuckets 已被外部设置为 null；English: specialBuckets has been set to null externally
     */
    public BucketConfig getBucketConfig(String bucketName) {
        BucketConfig bucketConfig = specialBuckets.get(bucketName);
        if (bucketConfig != null) return bucketConfig;
        return defaultBucketConfig;
    }

}

