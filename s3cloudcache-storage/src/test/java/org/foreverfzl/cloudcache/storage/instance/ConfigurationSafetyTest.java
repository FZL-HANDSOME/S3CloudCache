package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.storage.instance.cloudcache.S3CloudCacheInstance;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.*;

/** 配置回归只使用临时目录和禁止网络操作的客户端，不访问真实 S3 或用户 WAL。 */
/**
 * 中文：验证配置快照、必要容量约束与路径准入，尤其要求非法输入在创建 WAL、锁文件或映射前失败。
 * English: Verifies configuration snapshots, required capacity constraints, and path admission, especially failure before WAL, lock-file, or mapping creation.
 */
public class ConfigurationSafetyTest {
    /**
     * 中文：JUnit 管理的独立测试目录；每个非法配置使用新的子目录来检查是否产生副作用。
     * English: JUnit-managed isolated directory; each invalid configuration gets a fresh child for side-effect checks.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * 中文：复制配置后同时修改默认值、特殊配置、Map 和空闲阈值，验证快照没有可变对象共享。
     * English: Mutates defaults, named settings, the map, and idle threshold after copying to verify the snapshot shares no mutable configuration objects.
     */
    @Test
    public void snapshotDoesNotShareMutableBucketConfigurations() {
        BucketConfig originalDefault = validBucket();
        BucketConfig originalSpecial = validBucket().setS3KeyPrefix("special");
        originalSpecial.enableHeadCheck = true;
        originalSpecial.flushFileMetaInfoTime = 1234;
        originalSpecial.chackMappedFileTime = 2345;
        S3CloudCacheConfig original = new S3CloudCacheConfig("instance", null, originalDefault);
        original.blockMaxIdleTime = 3456;
        original.specialBuckets.put("special-bucket", originalSpecial);
        S3CloudCacheConfig snapshot = original.snapshotAndValidate();

        assertNotSame(originalDefault, snapshot.defaultBucketConfig);
        assertNotSame(original.specialBuckets, snapshot.specialBuckets);
        assertNotSame(originalSpecial, snapshot.getBucketConfig("special-bucket"));
        // 中文：既修改嵌套对象又清空源 Map，使浅复制或漏复制字段都能被断言发现。
        // English: Mutating nested objects and clearing the source map exposes shallow copies and omitted fields.
        originalDefault.blockSize = 4;
        originalDefault.s3KeyPrefix = "changed";
        originalSpecial.blockUpLoadCount = 0;
        original.specialBuckets.clear();
        original.blockMaxIdleTime = 1;

        assertEquals(Integer.valueOf(2 * 1024 * 1024), snapshot.defaultBucketConfig.blockSize);
        assertEquals("config-test", snapshot.defaultBucketConfig.s3KeyPrefix);
        assertEquals(Integer.valueOf(2), snapshot.getBucketConfig("special-bucket").blockUpLoadCount);
        assertEquals(Boolean.TRUE, snapshot.getBucketConfig("special-bucket").enableHeadCheck);
        assertEquals(Integer.valueOf(1234), snapshot.getBucketConfig("special-bucket").flushFileMetaInfoTime);
        assertEquals(Integer.valueOf(2345), snapshot.getBucketConfig("special-bucket").chackMappedFileTime);
        assertEquals(Integer.valueOf(3456), snapshot.blockMaxIdleTime);
    }

    /**
     * 中文：验证合法余数缓存与 2 GiB WAL 容量不会被过严的输入校验拒绝；不实际分配这些容量。
     * English: Ensures a cache remainder and 2 GiB WAL capacity are not rejected by overly strict validation, without allocating those capacities.
     */
    @Test
    public void cacheRemainderAndTwoGiBFileSizeRemainSupported() {
        BucketConfig config = validBucket();
        config.cacheSize = (long) config.blockSize + 17;
        config.walFileSize = 2L * 1024 * 1024 * 1024;
        BucketConfig copy = config.copyAndValidate();
        assertEquals(config.cacheSize, copy.cacheSize);
        assertEquals(config.walFileSize, copy.walFileSize);
    }

    /**
     * 中文：实例构造后破坏原配置，再延迟创建默认和特殊 Writer，验证运行实例始终使用原快照。
     * English: Corrupts caller-owned settings after construction, then lazily creates default and named Writers to verify the instance retains its original snapshot.
     * @throws Exception 中文：临时目录或实例资源操作失败；English: temporary-directory or instance-resource operations fail
     */
    @Test(timeout = 30000)
    public void liveInstanceKeepsItsOriginalDefaultAndSpecialConfiguration() throws Exception {
        Path root = temporary.newFolder("snapshot-integration").toPath();
        BucketConfig defaultBucket = validBucket();
        BucketConfig specialBucket = validBucket().setS3KeyPrefix("special");
        S3CloudCacheConfig original = new S3CloudCacheConfig("instance", root.toString(), defaultBucket);
        original.specialBuckets.put("special-bucket", specialBucket);
        S3CloudCacheInstance instance = new S3CloudCacheInstance(noNetworkClient(), original);
        try {
            // 故意将调用方对象改为非法状态，实例延迟创建 Bucket 时也只能使用已校验的快照。
            // 中文：注入的值会导致新快照验证失败；Writer 仍成功说明它没有再次读取用户对象。
            // English: Injected values would fail fresh validation; successful Writer creation proves it does not reread the caller's objects.
            defaultBucket.blockSize = 6;
            defaultBucket.s3KeyPrefix = null;
            specialBucket.blockUpLoadCount = 0;
            specialBucket.s3KeyPrefix = null;
            original.specialBuckets.clear();
            var normal = instance.getBucketWriterInstance("default-bucket");
            var special = instance.getBucketWriterInstance("special-bucket");
            assertEquals(Integer.valueOf(2 * 1024 * 1024), normal.getMappedManager().config.blockSize);
            assertEquals("config-test", normal.getMappedManager().config.s3KeyPrefix);
            assertEquals(Integer.valueOf(2), special.getMappedManager().config.blockUpLoadCount);
            assertEquals("special", special.getMappedManager().config.s3KeyPrefix);
        } finally {
            instance.close(1000, 1000, 1000);
        }
    }

    /**
     * 中文：分别用 ASCII 和多字节中文检验 4072 字节前缀边界，避免按字符数量误判元数据容量。
     * English: Checks the 4072-byte prefix limit with ASCII and multibyte Chinese text to avoid mistaking character counts for metadata capacity.
     */
    @Test
    public void prefixLimitCountsUtf8BytesRatherThanCharacters() {
        validBucket().setS3KeyPrefix("a".repeat(4072)).copyAndValidate();
        validBucket().setS3KeyPrefix("汉".repeat(1357)).copyAndValidate();
        assertThrows(IllegalArgumentException.class,
                () -> validBucket().setS3KeyPrefix("a".repeat(4073)).copyAndValidate());
        assertThrows(IllegalArgumentException.class,
                () -> validBucket().setS3KeyPrefix("汉".repeat(1358)).copyAndValidate());
    }

    /**
     * 中文：逐个注入 Bucket 尺寸、并发、时间、布尔和前缀错误，验证构造失败且目标目录仍为空。
     * English: Injects Bucket size, concurrency, timing, boolean, and prefix errors individually, verifying construction fails and the target directory stays empty.
     * @throws Exception 中文：测试目录创建或遍历失败；English: test-directory creation or traversal fails
     */
    @Test
    public void invalidBucketSettingsFailBeforeCreatingWalOrMetadata() throws Exception {
        List<Consumer<BucketConfig>> invalidSettings = List.of(
                b -> b.blockSize = null,
                b -> b.blockSize = 0,
                b -> b.blockSize = 6,
                b -> b.walFileSize = null,
                b -> b.walFileSize = -1L,
                b -> b.walFileSize = (long) b.blockSize + 1,
                b -> b.walFileSize = 1025L * b.blockSize,
                b -> b.cacheSize = null,
                b -> b.cacheSize = (long) b.blockSize - 1,
                b -> b.cacheSize = ((long) Integer.MAX_VALUE + 1) * b.blockSize,
                b -> b.blockUpLoadCount = null,
                b -> b.blockUpLoadCount = 0,
                b -> b.flushFileMetaInfoTime = 0,
                b -> b.chackMappedFileTime = -1,
                b -> b.isWarmWalFile = null,
                b -> b.isLockMappedFilePageCache = null,
                b -> b.enableHeadCheck = null,
                b -> b.s3KeyPrefix = null,
                b -> b.s3KeyPrefix = "x".repeat(4073));
        // 中文：每轮只改一个配置项并使用新目录，失败应来自该项而不是前一轮遗留状态。
        // English: Each iteration changes one setting in a fresh directory, isolating the failure from prior test state.
        for (int index = 0; index < invalidSettings.size(); index++) {
            Path root = temporary.newFolder("invalid-bucket-" + index).toPath();
            BucketConfig bucket = validBucket();
            invalidSettings.get(index).accept(bucket);
            S3CloudCacheConfig config = new S3CloudCacheConfig("instance", root.toString(), bucket);
            assertThrows(IllegalArgumentException.class, () -> new S3CloudCacheInstance(noNetworkClient(), config));
            assertDirectoryUntouched(root);
        }
    }

    /**
     * 中文：验证实例名、空闲阈值、默认配置和特殊配置表的非法状态在申请资源前被拒绝。
     * English: Verifies invalid instance names, idle thresholds, defaults, and named-configuration maps are rejected before resource acquisition.
     * @throws Exception 中文：测试目录创建或遍历失败；English: test-directory creation or traversal fails
     */
    @Test
    public void invalidInstanceSettingsFailBeforeCreatingResources() throws Exception {
        List<Consumer<S3CloudCacheConfig>> invalidSettings = List.of(
                c -> c.instanceName = null,
                c -> c.instanceName = " ",
                c -> c.blockMaxIdleTime = null,
                c -> c.blockMaxIdleTime = 0,
                c -> c.defaultBucketConfig = null,
                c -> c.specialBuckets = null,
                c -> c.specialBuckets.put("bucket", null),
                c -> c.specialBuckets.put("../escape", validBucket()));
        for (int index = 0; index < invalidSettings.size(); index++) {
            Path root = temporary.newFolder("invalid-instance-" + index).toPath();
            S3CloudCacheConfig config = new S3CloudCacheConfig("instance", root.toString(), validBucket());
            invalidSettings.get(index).accept(config);
            assertThrows(IllegalArgumentException.class, () -> new S3CloudCacheInstance(noNetworkClient(), config));
            assertDirectoryUntouched(root);
        }
    }

    /**
     * 中文：覆盖路径跳转、两种分隔符、驱动器标记与 NUL，同时保留正常中文和下划线名称。
     * English: Covers traversal, both separator styles, drive markers, and NUL while preserving valid Chinese and underscore names.
     * @throws Exception 中文：临时目录操作失败；English: temporary-directory operations fail
     */
    @Test
    public void instanceNamesCannotEscapeTheWalRoot() throws Exception {
        String[] invalidNames = {".", "..", "../escape", "..\\escape", "a/b", "a\\b", "/absolute", "C:drive", "a\0b"};
        for (int index = 0; index < invalidNames.length; index++) {
            Path root = temporary.newFolder("invalid-name-" + index).toPath();
            String name = invalidNames[index];
            assertThrows(IllegalArgumentException.class,
                    () -> S3CloudCacheConfig.validatePathComponent(name, "bucketName"));
            S3CloudCacheConfig config = new S3CloudCacheConfig(name, root.toString(), validBucket());
            assertThrows(IllegalArgumentException.class, () -> new S3CloudCacheInstance(noNetworkClient(), config));
            assertDirectoryUntouched(root);
        }
        S3CloudCacheConfig.validatePathComponent("正常_bucket-1.2", "bucketName");
    }

    /**
     * 中文：合法实例启动后再传入非法 Bucket 名，比较完整目录列表，验证 Writer 准入不产生越界目录。
     * English: Supplies invalid Bucket names after valid startup and compares directory listings to verify Writer admission creates no escaped paths.
     * @throws Exception 中文：目录遍历或实例生命周期操作失败；English: directory traversal or instance lifecycle operations fail
     */
    @Test(timeout = 30000)
    public void writerNamesCannotCreateDirectoriesOutsideTheInstance() throws Exception {
        Path root = temporary.newFolder("writer-paths").toPath();
        S3CloudCacheInstance instance = new S3CloudCacheInstance(noNetworkClient(),
                new S3CloudCacheConfig("instance", root.toString(), validBucket()));
        try {
            List<Path> originalPaths;
            try (var files = Files.walk(root)) {
                originalPaths = files.sorted().toList();
            }
            for (String name : List.of(".", "..", "../escape", "..\\escape", "a/b", "a\\b", "C:drive")) {
                assertThrows(IllegalArgumentException.class, () -> instance.getBucketWriterInstance(name));
            }
            try (var files = Files.walk(root)) {
                assertEquals("非法 Bucket 名不能触发 WAL 目录创建", originalPaths, files.sorted().toList());
            }
        } finally {
            instance.close(1000, 1000, 1000);
        }
    }

    /**
     * 中文：断言目录树只有预先创建的根目录，排除启动失败前已创建锁、WAL 或元数据的情况。
     * English: Asserts the tree contains only its precreated root, excluding lock, WAL, or metadata creation before startup failure.
     * @param root 中文：本用例新建的空目录；English: fresh empty directory owned by this case
     * @throws Exception 中文：目录遍历失败；English: directory traversal fails
     */
    private static void assertDirectoryUntouched(Path root) throws Exception {
        try (var files = Files.walk(root)) {
            assertEquals("配置失败不得创建实例目录、锁文件、WAL 或元数据", 1, files.count());
        }
    }

    /**
     * 中文：为每轮测试返回独立合法基线，采用两块缓存与两块 WAL、关闭预热以减少测试资源消耗。
     * English: Returns an independent valid baseline with two cache blocks, two WAL blocks, and warming disabled to limit test resources.
     * @return 中文：尚未分配资源的可变 Bucket 配置；English: mutable Bucket configuration that has allocated no resources
     */
    private static BucketConfig validBucket() {
        return new BucketConfig().setS3KeyPrefix("config-test")
                .setBlockSize(2 * 1024 * 1024)
                .setWalFileSize(4L * 1024 * 1024)
                .setCacheSize(4L * 1024 * 1024)
                .setBlockUpLoadCount(2).setWarmWalFile(false);
    }

    /**
     * 中文：构造只允许 close、服务名及 Object 基础方法的代理，任何实际 S3 API 调用都会使测试失败。
     * English: Builds a proxy allowing only close, service name, and basic Object methods; every actual S3 API call fails the test.
     * @return 中文：不会创建网络连接的 S3Client 测试替身；English: S3Client test double that opens no network connection
     */
    private static S3Client noNetworkClient() {
        return (S3Client) Proxy.newProxyInstance(S3Client.class.getClassLoader(), new Class<?>[]{S3Client.class},
                (proxy, method, args) -> {
                    return switch (method.getName()) {
                        case "close" -> null;
                        case "serviceName" -> "s3";
                        case "toString" -> "ConfigurationTestNoNetworkS3";
                        case "hashCode" -> System.identityHashCode(proxy);
                        case "equals" -> proxy == args[0];
                        default -> throw new AssertionError("配置校验不应执行 S3 操作: " + method.getName());
                    };
                });
    }
}
