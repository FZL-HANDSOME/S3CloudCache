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
public class ConfigurationSafetyTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

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

    @Test
    public void cacheRemainderAndTwoGiBFileSizeRemainSupported() {
        BucketConfig config = validBucket();
        config.cacheSize = (long) config.blockSize + 17;
        config.walFileSize = 2L * 1024 * 1024 * 1024;
        BucketConfig copy = config.copyAndValidate();
        assertEquals(config.cacheSize, copy.cacheSize);
        assertEquals(config.walFileSize, copy.walFileSize);
    }

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

    @Test
    public void prefixLimitCountsUtf8BytesRatherThanCharacters() {
        validBucket().setS3KeyPrefix("a".repeat(4072)).copyAndValidate();
        validBucket().setS3KeyPrefix("汉".repeat(1357)).copyAndValidate();
        assertThrows(IllegalArgumentException.class,
                () -> validBucket().setS3KeyPrefix("a".repeat(4073)).copyAndValidate());
        assertThrows(IllegalArgumentException.class,
                () -> validBucket().setS3KeyPrefix("汉".repeat(1358)).copyAndValidate());
    }

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
        for (int index = 0; index < invalidSettings.size(); index++) {
            Path root = temporary.newFolder("invalid-bucket-" + index).toPath();
            BucketConfig bucket = validBucket();
            invalidSettings.get(index).accept(bucket);
            S3CloudCacheConfig config = new S3CloudCacheConfig("instance", root.toString(), bucket);
            assertThrows(IllegalArgumentException.class, () -> new S3CloudCacheInstance(noNetworkClient(), config));
            assertDirectoryUntouched(root);
        }
    }

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

    private static void assertDirectoryUntouched(Path root) throws Exception {
        try (var files = Files.walk(root)) {
            assertEquals("配置失败不得创建实例目录、锁文件、WAL 或元数据", 1, files.count());
        }
    }

    private static BucketConfig validBucket() {
        return new BucketConfig().setS3KeyPrefix("config-test")
                .setBlockSize(2 * 1024 * 1024)
                .setWalFileSize(4L * 1024 * 1024)
                .setCacheSize(4L * 1024 * 1024)
                .setBlockUpLoadCount(2).setWarmWalFile(false);
    }

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
