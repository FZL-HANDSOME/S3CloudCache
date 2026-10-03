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
/**
 * 中文：覆盖同 JVM 和独立 JVM 的本地目录独占，以及关闭失败/超时期间的所有权；这不是分布式 S3 命名空间锁测试。
 * English: Covers local-directory ownership within and across JVMs and during failed/timed-out shutdown; this does not test a distributed S3 namespace lock.
 */
public class InstanceDirectoryLockTest {
    /**
     * 中文：生成小容量且不预热、不锁页的配置，测试只创建目录和映射，不写待上传数据。
     * English: Creates a small configuration with warming and page locking disabled; tests create directories and mappings without data awaiting upload.
     * @param directory 中文：测试专用基础目录；English: test-only base directory
     * @param name 中文：待竞争或隔离的实例名；English: instance name used for contention or isolation
     * @return 中文：两块 WAL 和两块缓存的实例配置；English: instance configuration with two WAL and two cache blocks
     */
    private static S3CloudCacheConfig config(Path directory, String name) {
        BucketConfig bucket = new BucketConfig("lock-test/", 8192L, 8192L, 4096, 1, false, false);
        return new S3CloudCacheConfig(name, directory.toString(), bucket);
    }

    /**
     * 中文：第一实例持锁时拒绝第二实例且不改原元数据；完整关闭后可重开，同时锁文件本身继续保留。
     * English: Rejects a second owner without modifying metadata while the first holds the lock; reopening succeeds after full close while the lock file remains.
     * @throws Exception 中文：临时目录、元数据读取或关闭操作失败；English: temporary-directory, metadata-read, or shutdown operations fail
     */
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

    /**
     * 中文：同一基础目录下的不同实例名可以并存，关闭其中一个不影响另一实例创建 Writer。
     * English: Distinct instance names may coexist under one base directory, and closing one does not prevent the other from creating a Writer.
     * @throws Exception 中文：临时目录或实例生命周期操作失败；English: temporary-directory or instance lifecycle operations fail
     */
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

    /**
     * 中文：注入仍在途的写入令零等待关闭失败，验证目录所有权保留，待在途计数归零后重试可释放。
     * English: Injects an in-flight write so zero-wait close fails, verifying ownership is retained until the count returns to zero and a retry releases it.
     * @throws Exception 中文：反射注入、临时目录或实例关闭失败；English: reflective injection, temporary-directory, or instance-close operations fail
     */
    @Test(timeout = 30000)
    public void closeTimeoutKeepsDirectoryOwnershipUntilRetryFinishes() throws Exception {
        Path directory = Files.createTempDirectory("cloudcache-lock-timeout-");
        S3CloudCacheInstance first = new S3CloudCacheInstance(null, config(directory, "same"));
        BucketWriterWriter writer = first.getBucketWriterInstance("bucket");
        var inFlight = BucketWriterWriter.class.getDeclaredField("inFlightWrites");
        inFlight.setAccessible(true);
        try {
            // 注入一个未结束的准入写入，避免用 sleep 猜测关闭竞态。
            // 中文：仅测试反射修改计数；finally 恢复到零，不能让故障注入永久阻止资源释放。
            // English: Only the test mutates this count reflectively; finally restores zero so the injected fault cannot permanently block cleanup.
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

    /**
     * 中文：在父 JVM 持锁期间启动相同 classpath 的子 JVM，要求其获取相同实例目录失败并返回约定退出码。
     * English: Starts a child JVM with the same classpath while the parent owns the directory, requiring acquisition failure and the agreed exit code.
     * @throws Exception 中文：进程启动、等待或资源操作失败；English: process startup, waiting, or resource operations fail
     */
    @Test(timeout = 30000)
    public void anotherJvmCannotAcquireTheSameDirectory() throws Exception {
        Path directory = Files.createTempDirectory("cloudcache-process-lock-");
        S3CloudCacheInstance first = new S3CloudCacheInstance(null, config(directory, "same"));
        Process process = null;
        try {
            String executable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            // 中文：独立进程不能共享父进程中的静态注册表，因此可以检查排他行为是否跨 JVM 生效。
            // English: A separate process cannot share the parent's static registry, allowing exclusivity to be checked across JVMs.
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

    /**
     * 中文：由独立 JVM 启动的探针入口，只尝试获取父测试持有的同名实例目录。
     * English: Probe entry point launched in a separate JVM, attempting to acquire the same instance directory held by the parent test.
     */
    public static class LockProbe {
        /**
         * 中文：获取失败并抛 CloudCacheException 时以 17 退出；意外获取成功则正常关闭后退出，供父测试判错。
         * English: Exits with 17 on CloudCacheException; unexpected acquisition success closes normally so the parent can reject that outcome.
         * @param args 中文：第一个参数为父测试的基础目录；English: first argument is the parent test's base directory
         */
        public static void main(String[] args) {
            try {
                S3CloudCacheInstance instance = new S3CloudCacheInstance(null, config(Path.of(args[0]), "same"));
                instance.close(1000, 1000, 1000);
            } catch (CloudCacheException expected) {
                System.exit(17);
            }
        }
    }

    /**
     * 中文：模拟存储资源已关闭后 S3Client.close 抛异常，验证再次关闭不访问已卸载元数据且目录锁已可重获。
     * English: Simulates S3Client.close failure after storage cleanup, verifying repeated close avoids unmapped metadata and directory ownership can be reacquired.
     * @throws Exception 中文：临时目录或实例资源操作失败；English: temporary-directory or instance-resource operations fail
     */
    @Test(timeout = 30000)
    public void clientCloseFailureDoesNotStrandWalOwnership() throws Exception {
        Path directory = Files.createTempDirectory("cloudcache-client-close-lock-");
        // 中文：代理仅在 close 上注入失败，任何其它 S3 方法都不允许，排除网络状态对关闭测试的干扰。
        // English: The proxy injects failure only on close and permits no other S3 method, keeping network state out of the shutdown test.
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
