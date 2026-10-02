package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.storage.instance.bucket.BucketWriterWriter;
import org.foreverfzl.cloudcache.storage.instance.cloudcache.S3CloudCacheInstance;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.foreverfzl.cloudchache.common.exception.CloudCacheException;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/** 只操作独立临时目录，不连接 S3；证明独占权覆盖实例的整个资源生命周期。 */
public class InstanceDirectoryLockTest {
    private static S3CloudCacheConfig config(Path directory, String name) {
        BucketConfig bucket = new BucketConfig("lock-test/", 8192L, 8192L, 4096, 1, false, false);
        return new S3CloudCacheConfig(name, directory.toString(), bucket);
    }

    @Test(timeout = 30000)
    public void secondInstanceCannotTouchOwnedWalAndCanOpenAfterClose() throws Exception {
        Path directory = Files.createTempDirectory("cloudcache-lock-");
        S3CloudCacheInstance first = new S3CloudCacheInstance(null, config(directory, "same"));
        try {
            first.getBucketWriterInstance("bucket");
            Path meta = Path.of(directory + ProjectUtil.WAL_FILE_ADDRESS, "same", "bucket", "bucketMeta");
            byte[] original = Files.readAllBytes(meta);
            assertThrows(CloudCacheException.class, () -> new S3CloudCacheInstance(null, config(directory, "same")));
            assertArrayEquals("被拒绝的实例不能改写正在使用的元数据", original, Files.readAllBytes(meta));
        } finally {
            first.close(1000, 1000, 1000);
        }
        S3CloudCacheInstance reopened = new S3CloudCacheInstance(null, config(directory, "same"));
        reopened.close(1000, 1000, 1000);
        assertTrue(Files.exists(Path.of(directory + ProjectUtil.WAL_FILE_ADDRESS, "same", ".instance.lock")));
    }

    @Test(timeout = 30000)
    public void differentInstancesMayShareBaseDirectory() throws Exception {
        Path directory = Files.createTempDirectory("cloudcache-independent-locks-");
        S3CloudCacheInstance first = new S3CloudCacheInstance(null, config(directory, "first"));
        S3CloudCacheInstance second = new S3CloudCacheInstance(null, config(directory, "second"));
        try {
            first.close(1000, 1000, 1000);
            second.getBucketWriterInstance("bucket");
        } finally {
            first.close(1000, 1000, 1000);
            second.close(1000, 1000, 1000);
        }
    }

    @Test(timeout = 30000)
    public void closeTimeoutKeepsDirectoryOwnershipUntilRetryFinishes() throws Exception {
        Path directory = Files.createTempDirectory("cloudcache-lock-timeout-");
        S3CloudCacheInstance first = new S3CloudCacheInstance(null, config(directory, "same"));
        BucketWriterWriter writer = first.getBucketWriterInstance("bucket");
        var inFlight = BucketWriterWriter.class.getDeclaredField("inFlightWrites");
        inFlight.setAccessible(true);
        try {
            // 注入一个未结束的准入写入，避免用 sleep 猜测关闭竞态。
            inFlight.setInt(writer, 1);
            assertThrows(IllegalStateException.class, () -> first.close(0, 0, 0));
            assertThrows(CloudCacheException.class, () -> new S3CloudCacheInstance(null, config(directory, "same")));
        } finally {
            inFlight.setInt(writer, 0);
            first.close(1000, 1000, 1000);
        }
        S3CloudCacheInstance reopened = new S3CloudCacheInstance(null, config(directory, "same"));
        reopened.close(1000, 1000, 1000);
    }

    @Test(timeout = 30000)
    public void anotherJvmCannotAcquireTheSameDirectory() throws Exception {
        Path directory = Files.createTempDirectory("cloudcache-process-lock-");
        S3CloudCacheInstance first = new S3CloudCacheInstance(null, config(directory, "same"));
        Process process = null;
        try {
            String executable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            process = new ProcessBuilder(executable, "-cp", System.getProperty("java.class.path"),
                    LockProbe.class.getName(), directory.toString()).redirectErrorStream(true).start();
            assertTrue("锁检查子进程超时", process.waitFor(15, TimeUnit.SECONDS));
            String output = new String(process.getInputStream().readAllBytes());
            assertEquals(output, 17, process.exitValue());
        } finally {
            if (process != null && process.isAlive()) process.destroyForcibly();
            first.close(1000, 1000, 1000);
        }
    }

    public static class LockProbe {
        public static void main(String[] args) {
            try {
                S3CloudCacheInstance instance = new S3CloudCacheInstance(null, config(Path.of(args[0]), "same"));
                instance.close(1000, 1000, 1000);
            } catch (CloudCacheException expected) {
                System.exit(17);
            }
        }
    }

    @Test(timeout = 30000)
    public void clientCloseFailureDoesNotStrandWalOwnership() throws Exception {
        Path directory = Files.createTempDirectory("cloudcache-client-close-lock-");
        S3Client client = (S3Client) Proxy.newProxyInstance(S3Client.class.getClassLoader(),
                new Class<?>[]{S3Client.class}, (proxy, method, args) -> {
                    if (method.getName().equals("close")) throw new IllegalStateException("injected client close failure");
                    throw new UnsupportedOperationException(method.getName());
                });
        S3CloudCacheInstance first = new S3CloudCacheInstance(client, config(directory, "same"));
        first.getBucketWriterInstance("bucket");
        assertThrows(IllegalStateException.class, () -> first.close(1000, 1000, 1000));
        first.close(1000, 1000, 1000); // 存储层已关完，不重复访问卸载后的元数据。
        S3CloudCacheInstance reopened = new S3CloudCacheInstance(null, config(directory, "same"));
        reopened.close(1000, 1000, 1000);
    }
}
